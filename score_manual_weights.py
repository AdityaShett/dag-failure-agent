"""
score_manual_weights.py

Replaces tune_weights.py / tune_weights_3signal.py / tune_weights_v2.py.
No grid search -- just a fixed, manually-chosen weighting for the three
remaining signals (history, logs, source), scored for accuracy, precision,
and F1 against your labeled data.

This is intentionally "manageable, not optimal" per the new plan: a human
picks numbers that make sense, then this script tells you how that specific
choice performs, rather than searching thousands of combinations.

CAVEAT: with very few merged examples, precision/F1 are unstable -- a single
example flipping above/below threshold can swing them from 0.0 to 1.0. Don't
treat these numbers as settled until you have a meaningful count on both
sides (framework here uses the same >=5/class floor as before to warn you,
but will still compute and show the numbers below that if you want to look).

Usage:
    python score_manual_weights.py
    python score_manual_weights.py --history 0.5 --logs 0.3 --source 0.2 --threshold 0.6
"""
import argparse
import os

from google.cloud import bigquery

PROJECT = os.environ["GCP_PROJECT"]
DATASET = "dag_failure_agent"
MIN_PER_CLASS_WARNING = 5


def fetch_rows():
    client = bigquery.Client(project=PROJECT)
    sql = f"""
        WITH latest_outcome AS (
          SELECT record_id, outcome,
                 ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY updated_at DESC) AS rn
          FROM `{PROJECT}.{DATASET}.confidence_outcomes`
        )
        SELECT s.s_history, s.s_logs, s.s_source,
               IF(COALESCE(o.outcome, s.outcome) = 'merged', 1, 0) AS merged
        FROM `{PROJECT}.{DATASET}.confidence_signals` s
        LEFT JOIN latest_outcome o ON o.record_id = s.record_id AND o.rn = 1
        WHERE COALESCE(o.outcome, s.outcome) IN ('merged', 'rejected')
    """
    return [dict(r) for r in client.query(sql).result()]


def score(row, weights):
    return (
        weights["history"] * row["s_history"]
        + weights["logs"] * row["s_logs"]
        + weights["source"] * row["s_source"]
    )


def confusion(rows, weights, threshold):
    tp = fp = tn = fn = 0
    for r in rows:
        pred = score(r, weights) >= threshold
        actual = bool(r["merged"])
        if pred and actual:
            tp += 1
        elif pred and not actual:
            fp += 1
        elif not pred and actual:
            fn += 1
        else:
            tn += 1
    return tp, fp, tn, fn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", type=float, default=0.40)
    parser.add_argument("--logs", type=float, default=0.30)
    parser.add_argument("--source", type=float, default=0.30)
    parser.add_argument("--threshold", type=float, default=0.50)
    args = parser.parse_args()

    weights = {"history": args.history, "logs": args.logs, "source": args.source}
    total_weight = sum(weights.values())
    if abs(total_weight - 1.0) > 0.01:
        print(f"NOTE: weights sum to {total_weight:.2f}, not 1.0 -- scores won't be on a clean 0-1 scale.")

    rows = fetch_rows()
    n_merged = sum(r["merged"] for r in rows)
    n_rejected = len(rows) - n_merged

    print(f"Data: {len(rows)} labeled rows ({n_merged} merged, {n_rejected} rejected)")
    if n_merged < MIN_PER_CLASS_WARNING or n_rejected < MIN_PER_CLASS_WARNING:
        print(
            f"WARNING: fewer than {MIN_PER_CLASS_WARNING} examples in one class. "
            f"The metrics below are being computed anyway, but treat them as "
            f"provisional, not a settled result."
        )

    print(f"\nWeights: {weights}")
    print(f"Threshold: {args.threshold}\n")

    tp, fp, tn, fn = confusion(rows, weights, args.threshold)

    accuracy = (tp + tn) / len(rows) if rows else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    trivial_baseline = n_rejected / len(rows) if rows else 0.0

    print("--- Confusion matrix ---")
    print(f"TP={tp}  FP={fp}  TN={tn}  FN={fn}")

    print("\n--- Scores ---")
    print(f"accuracy:                   {accuracy:.3f}")
    print(f"precision:                  {precision:.3f}")
    print(f"recall:                     {recall:.3f}")
    print(f"f1:                         {f1:.3f}")
    print(f"trivial always-reject acc.: {trivial_baseline:.3f}")

    if accuracy < trivial_baseline:
        print(
            "\nNOTE: accuracy is below the trivial always-reject baseline. "
            "Expected at very low merged-counts -- any attempt to catch the "
            "rare positive case costs some accuracy. Watch precision/recall "
            "instead of accuracy alone while merged examples are this scarce."
        )


if __name__ == "__main__":
    main()
