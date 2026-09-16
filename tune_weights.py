#!/usr/bin/env python3
"""
tune_weights.py -- the one weight tuner.

Replaces tune_weights.py (old), tune_weights_v2.py, tune_weights_3signal.py
and score_manual_weights.py. Those disagreed with each other about how many
signals there were, what shape weights.json should be, and whether the
threshold was tunable -- and none of them wrote a file that
processor/agent/nodes.py could actually read.

Three things it does:

  score   Score a specific set of weights against labeled data and print the
          confusion matrix (what score_manual_weights.py did).
  fit     Search for the weights + threshold that maximise an objective,
          with k-fold cross-validation (what tune_weights_3signal.py tried
          to do), and optionally write config/weights.json.
  compare Score the current config/weights.json against a fitted candidate
          side by side, so you can see what you'd be trading away.

Data sources:

  --source fixtures   config/tuning_fixtures.json + the local logs/DAGs.
                      Works offline, no GCP. This is the one to use until
                      there are enough human-labeled PR outcomes.
  --source results    a results.json produced by run_benchmark_local.py or
                      export_results.py (uses expected_outcome as the label).
  --source bigquery   confidence_signals joined to the latest
                      confidence_outcomes row per record_id; label is
                      merged=1 / rejected=0. Needs the 001 migration applied
                      and enough labeled PRs.

Usage:
    python tune_weights.py score
    python tune_weights.py fit --objective f1
    python tune_weights.py fit --apply
    python tune_weights.py fit --source bigquery --min-per-class 5
    python tune_weights.py compare
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent / "processor"))

from agent import confidence  # noqa: E402

DEFAULT_WEIGHTS_PATH = "config/weights.json"
DEFAULT_FIXTURES_PATH = "config/tuning_fixtures.json"

POSITIVE = list(confidence.POSITIVE_SIGNALS)
PENALTY = list(confidence.PENALTY_SIGNALS)


# --------------------------------------------------------------------------
# Data loading -- every source returns [{"id", "signals", "label", "group"}]
# --------------------------------------------------------------------------

def load_from_fixtures(fixtures_path: str, weight_tests_dir: str,
                       repo_root: str) -> List[dict]:
    data = json.loads(Path(fixtures_path).read_text(encoding="utf-8"))
    rows = []
    for fx in data["fixtures"]:
        log_path = Path(weight_tests_dir) / fx["log_file"]
        if not log_path.exists():
            print(f"  skipping {fx['id']}: {log_path} not found")
            continue
        task_logs = log_path.read_text(encoding="utf-8", errors="replace")

        dag_source = ""
        if fx.get("code_file"):
            src_path = Path(repo_root) / fx["code_file"]
            if src_path.exists():
                dag_source = src_path.read_text(encoding="utf-8", errors="replace")

        rows.append({
            "id": fx["id"],
            "group": fx.get("group", "fixture"),
            "label": int(fx["label"]),
            "signals": confidence.extract_signals(
                task_logs=task_logs, dag_source=dag_source,
                dag_id=fx.get("dag_id", ""), task_id=fx.get("task_id", ""),
            ),
            "notes": fx.get("notes", ""),
        })
    return rows


def load_from_results(results_path: str) -> List[dict]:
    raw = json.loads(Path(results_path).read_text(encoding="utf-8"))
    runs = raw["runs"] if isinstance(raw, dict) else raw
    rows = []
    for run in runs:
        signals = run.get("signals")
        if not signals:
            continue
        label = run.get("label")
        if label is None:
            expected = run.get("expected_outcome")
            if expected not in ("PR_CREATED", "NO_CONFIDENT_FIX"):
                continue
            label = 1 if expected == "PR_CREATED" else 0
        rows.append({
            "id": run.get("run_id") or run.get("scenario_id", "?"),
            "group": run.get("mode", "results"),
            "label": int(label),
            "signals": signals,
            "notes": run.get("scenario_id", ""),
        })
    return rows


def load_from_bigquery(project: str, dataset: str) -> List[dict]:
    from google.cloud import bigquery  # lazy: only needed for this source

    sql = f"""
        WITH latest_outcome AS (
            SELECT record_id, outcome,
                   ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY updated_at DESC) AS rn
            FROM `{project}.{dataset}.confidence_outcomes`
        )
        SELECT
            s.record_id,
            s.scenario_id,
            s.s_stack_trace_present,
            s.s_line_number_matches_source,
            s.s_known_fix_pattern_match,
            s.s_log_completeness,
            s.s_history_merge_rate,
            s.s_retrieval_support,
            s.s_external_dependency_detected,
            COALESCE(o.outcome, s.outcome) AS final_outcome
        FROM `{project}.{dataset}.confidence_signals` s
        LEFT JOIN latest_outcome o ON o.record_id = s.record_id AND o.rn = 1
        WHERE COALESCE(o.outcome, s.outcome) IN ('merged', 'rejected', 'closed')
    """
    client = bigquery.Client(project=project)
    rows = []
    skipped = 0
    for r in client.query(sql).result():
        signals = {name: r.get(f"s_{name}") for name in POSITIVE + PENALTY}
        if signals["s_line_number_matches_source"
                  ] if False else all(signals[n] is None for n in POSITIVE):
            skipped += 1
            continue
        rows.append({
            "id": r["record_id"],
            "group": "bigquery",
            "label": 1 if r["final_outcome"] == "merged" else 0,
            "signals": signals,
            "notes": r.get("scenario_id") or "",
        })
    if skipped:
        print(f"  skipped {skipped} row(s) with no populated signal columns "
              f"(pre-migration rows -- see migrations/001_*.sql)")
    return rows


# --------------------------------------------------------------------------
# Metrics
# --------------------------------------------------------------------------

def build_cfg(positive: Dict[str, float], penalty: Dict[str, float],
              threshold: float, tiers: Optional[dict] = None) -> dict:
    return {
        "version": 2,
        "signal_weights": dict(positive),
        "penalty_weights": dict(penalty),
        "confidence_threshold": round(threshold, 4),
        "tier_thresholds": tiers or {
            "high": round(min(0.95, threshold + 0.15), 4),
            "medium": round(max(0.05, threshold - 0.10), 4),
        },
    }


def score_rows(rows: List[dict], cfg: dict) -> List[float]:
    return [confidence.score_signals(r["signals"], cfg)["score"] for r in rows]


def metrics(rows: List[dict], scores: List[float], threshold: float) -> dict:
    tp = fp = tn = fn = 0
    for row, score in zip(rows, scores):
        predicted = score >= threshold
        actual = bool(row["label"])
        if predicted and actual:
            tp += 1
        elif predicted and not actual:
            fp += 1
        elif not predicted and actual:
            fn += 1
        else:
            tn += 1
    n = max(1, tp + fp + tn + fn)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    specificity = tn / (tn + fp) if (tn + fp) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {
        "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "accuracy": (tp + tn) / n,
        "precision": precision,
        "recall": recall,
        "specificity": specificity,
        "balanced_accuracy": (recall + specificity) / 2,
        "f1": f1,
    }


def separation(rows: List[dict], scores: List[float], threshold: float) -> float:
    """How much room the decision has. Two weightings can both get every
    fixture right while one of them squeaks past by 0.001; this is the
    tie-breaker that prefers the one with daylight around the threshold.
    Returns the smaller of (gap below the threshold, gap above it), so a
    cut that sits right on top of a data point scores 0."""
    if not scores:
        return 0.0
    below = [threshold - s for s in scores if s < threshold]
    above = [s - threshold for s in scores if s >= threshold]
    return min(min(below) if below else 1.0, min(above) if above else 1.0)


def objective_value(m: dict, objective: str) -> float:
    if objective == "accuracy":
        return m["accuracy"]
    if objective == "balanced_accuracy":
        return m["balanced_accuracy"]
    if objective == "precision":
        # Precision alone is trivially maximised by predicting nothing, so
        # break ties toward keeping some recall.
        return m["precision"] + 0.01 * m["recall"]
    return m["f1"]


# --------------------------------------------------------------------------
# Fitting
# --------------------------------------------------------------------------

def best_threshold(rows: List[dict], scores: List[float], objective: str,
                   lo: float = 0.05, hi: float = 0.95) -> tuple:
    """Sweeps every threshold that could change a decision (the midpoints
    between adjacent observed scores) rather than an arbitrary grid."""
    candidates = sorted({round(s, 4) for s in scores})
    cuts = [lo, hi]
    for i, s in enumerate(candidates):
        cuts.append(min(hi, max(lo, s)))
        if i + 1 < len(candidates):
            cuts.append(min(hi, max(lo, (s + candidates[i + 1]) / 2)))
    best = (None, -1.0, None)
    for t in sorted(set(round(c, 4) for c in cuts)):
        m = metrics(rows, scores, t)
        # Tie-break on separation so a fit that only wins by a rounding
        # error doesn't beat one with real margin. The coefficient is small
        # enough that it can never outrank an actual accuracy difference.
        v = objective_value(m, objective) + 0.01 * separation(rows, scores, t)
        if v > best[1]:
            best = (t, v, m)
    return best


def random_weights(rng: random.Random, names: List[str]) -> Dict[str, float]:
    raw = [rng.random() ** 1.5 for _ in names]
    total = sum(raw) or 1.0
    return {n: v / total for n, v in zip(names, raw)}


def concentration_penalty(weights: Dict[str, float]) -> float:
    """Distance from an even split across the active signals.

    With a fixture set this small, an unregularised search reliably
    collapses onto a single signal (it can separate 17 rows with
    line_number_matches_source alone and nothing else). That scores
    perfectly here and is brittle everywhere else -- one signal extraction
    bug and the agent has no second opinion. This term prices that in, so a
    concentrated solution has to actually beat a balanced one on the data
    rather than merely tie it.
    """
    if not weights:
        return 0.0
    even = 1.0 / len(weights)
    return sum((w - even) ** 2 for w in weights.values()) ** 0.5


def fit(rows: List[dict], objective: str = "f1", restarts: int = 40,
        iterations: int = 250, seed: int = 42,
        fixed_penalty: Optional[float] = None,
        regularization: float = 0.08) -> dict:
    """Random-restart coordinate search over the weight simplex plus the
    penalty magnitude, with the threshold chosen optimally for each
    candidate. Pure stdlib on purpose: this has to be runnable in the same
    container as the worker, and scipy/sklearn were only ever used here for
    a 3-variable search.
    """
    rng = random.Random(seed)
    active = [n for n in POSITIVE
              if any(r["signals"].get(n) is not None for r in rows)]
    if not active:
        raise SystemExit("No positive signals are populated in this dataset.")
    dropped = [n for n in POSITIVE if n not in active]
    if dropped:
        print(f"  note: no data for {', '.join(dropped)} -- "
              f"keeping their current weights, fitting the rest")

    penalty_grid = ([fixed_penalty] if fixed_penalty is not None
                    else [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.8, 1.0])

    best = {"objective": -1.0}
    for restart in range(restarts):
        weights = (random_weights(rng, active) if restart else
                   {n: 1.0 / len(active) for n in active})
        penalty = rng.choice(penalty_grid)
        step = 0.25
        current = -1.0

        for _ in range(iterations):
            improved = False
            for name in active + ["__penalty__"]:
                for direction in (1, -1):
                    trial_w = dict(weights)
                    trial_p = penalty
                    if name == "__penalty__":
                        trial_p = max(0.0, min(1.0, penalty + direction * step))
                    else:
                        trial_w[name] = max(0.0, weights[name] + direction * step)
                        total = sum(trial_w.values())
                        if total <= 0:
                            continue
                        trial_w = {k: v / total for k, v in trial_w.items()}

                    cfg = build_cfg(trial_w,
                                    {"external_dependency_detected": -trial_p}, 0.5)
                    scores = score_rows(rows, cfg)
                    t, value, _ = best_threshold(rows, scores, objective)
                    value -= regularization * concentration_penalty(trial_w)
                    if value > current + 1e-9:
                        current, weights, penalty = value, trial_w, trial_p
                        best_t = t
                        improved = True
            if not improved:
                step /= 2
                if step < 0.01:
                    break

        cfg = build_cfg(weights, {"external_dependency_detected": -penalty}, 0.5)
        scores = score_rows(rows, cfg)
        t, value, m = best_threshold(rows, scores, objective)
        value -= regularization * concentration_penalty(weights)
        if value > best["objective"]:
            best = {"objective": value, "weights": weights, "penalty": penalty,
                    "threshold": t, "metrics": m}

    return best


def cross_validate(rows: List[dict], objective: str, k: int = 5,
                   seed: int = 42, **fit_kwargs) -> Optional[dict]:
    if len(rows) < k * 2:
        print(f"  skipping cross-validation: {len(rows)} rows is too few for "
              f"{k} folds (need >= {k * 2}).")
        return None
    rng = random.Random(seed)
    shuffled = rows[:]
    rng.shuffle(shuffled)
    folds = [shuffled[i::k] for i in range(k)]

    values = []
    for i in range(k):
        test = folds[i]
        train = [r for j, f in enumerate(folds) if j != i for r in f]
        if not test or len({r["label"] for r in train}) < 2:
            continue
        result = fit(train, objective=objective, restarts=8, iterations=120,
                     seed=seed + i, **fit_kwargs)
        cfg = build_cfg(result["weights"],
                        {"external_dependency_detected": -result["penalty"]},
                        result["threshold"])
        m = metrics(test, score_rows(test, cfg), result["threshold"])
        v = objective_value(m, objective)
        values.append(v)
        print(f"  fold {i + 1}: {objective}={v:.3f} (n_test={len(test)})")

    if not values:
        return None
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return {"mean": mean, "std": var ** 0.5, "folds": len(values)}


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------

def print_report(rows: List[dict], cfg: dict, title: str, per_row: bool = True) -> dict:
    scores = score_rows(rows, cfg)
    threshold = cfg["confidence_threshold"]
    m = metrics(rows, scores, threshold)

    print(f"\n=== {title} ===")
    print("  weights:  " + ", ".join(
        f"{k}={v:.3f}" for k, v in sorted(cfg["signal_weights"].items())))
    print("  penalty:  " + ", ".join(
        f"{k}={v:.3f}" for k, v in cfg["penalty_weights"].items()) or "  penalty: none")
    print(f"  threshold: {threshold:.3f}")
    print(f"  TP={m['tp']} FP={m['fp']} TN={m['tn']} FN={m['fn']}")
    print(f"  accuracy={m['accuracy']:.3f}  precision={m['precision']:.3f}  "
          f"recall={m['recall']:.3f}  f1={m['f1']:.3f}  "
          f"balanced_acc={m['balanced_accuracy']:.3f}")

    if per_row:
        print(f"\n  {'fixture':<28} {'label':>5} {'score':>7} {'decision':>9}  result")
        for row, score in sorted(zip(rows, scores), key=lambda p: -p[1]):
            predicted = score >= threshold
            ok = predicted == bool(row["label"])
            decision = "PR" if predicted else "no-PR"
            print(f"  {row['id'][:28]:<28} {row['label']:>5} {score:>7.3f} "
                  f"{decision:>9}  {'ok' if ok else 'MISS'}")
    return m


def sanity_check(rows: List[dict]) -> None:
    n_pos = sum(r["label"] for r in rows)
    n_neg = len(rows) - n_pos
    print(f"Loaded {len(rows)} labeled rows ({n_pos} positive, {n_neg} negative)")
    if n_pos == 0 or n_neg == 0:
        raise SystemExit("ERROR: need both classes present to tune anything.")

    # If a signal never varies it cannot contribute to a decision, and any
    # weight assigned to it is noise. Say so rather than fitting it anyway.
    for name in POSITIVE + PENALTY:
        values = {r["signals"].get(name) for r in rows}
        values.discard(None)
        if values and len(values) == 1:
            print(f"  WARNING: '{name}' is constant at {values.pop()} across this "
                  f"dataset -- its weight is unidentifiable from this data.")
        if not values:
            print(f"  note: '{name}' has no data in this dataset.")


def load_rows(args) -> List[dict]:
    if args.source == "fixtures":
        return load_from_fixtures(args.fixtures, args.weight_tests_dir, args.repo_root)
    if args.source == "results":
        return load_from_results(args.results)
    if args.source == "bigquery":
        project = args.project or os.environ.get("GCP_PROJECT")
        if not project:
            raise SystemExit("--project or GCP_PROJECT is required for --source bigquery")
        rows = load_from_bigquery(project, args.dataset)
        pos = sum(r["label"] for r in rows)
        neg = len(rows) - pos
        if min(pos, neg) < args.min_per_class:
            raise SystemExit(
                f"ERROR: need at least {args.min_per_class} of each class; have "
                f"merged={pos}, rejected={neg}. Label more PRs "
                f"(review_and_label_prs.py) or tune with --source fixtures.")
        return rows
    raise SystemExit(f"unknown source {args.source}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["score", "fit", "compare"])
    parser.add_argument("--source", default="fixtures",
                        choices=["fixtures", "results", "bigquery"])
    parser.add_argument("--fixtures", default=DEFAULT_FIXTURES_PATH)
    parser.add_argument("--results", default="results/results.json")
    parser.add_argument("--weight-tests-dir", default="weight_tests")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--project", default=None)
    parser.add_argument("--dataset", default=os.environ.get("BQ_DATASET", "dag_failure_agent"))
    parser.add_argument("--min-per-class", type=int, default=5)
    parser.add_argument("--weights", default=DEFAULT_WEIGHTS_PATH)
    parser.add_argument("--objective", default="f1",
                        choices=["f1", "accuracy", "balanced_accuracy", "precision"])
    parser.add_argument("--restarts", type=int, default=40)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--no-cv", action="store_true")
    parser.add_argument("--regularization", type=float, default=0.08,
                        help="How hard to push fitted weights toward an even "
                             "split (0 = pure accuracy chasing, which overfits "
                             "badly on small fixture sets)")
    parser.add_argument("--fix-penalty", type=float, default=None,
                        help="Hold the external-dependency penalty magnitude fixed "
                             "(e.g. 0.4) instead of fitting it")
    parser.add_argument("--threshold", type=float, default=None,
                        help="score/compare: override the threshold being scored")
    parser.add_argument("--apply", action="store_true",
                        help="fit: write the fitted weights to --weights")
    args = parser.parse_args()

    rows = load_rows(args)
    sanity_check(rows)

    current = confidence.load_weights(args.weights, strict=True)
    if args.threshold is not None:
        current = dict(current, confidence_threshold=args.threshold)

    if args.command == "score":
        print_report(rows, current, f"current weights ({args.weights})")
        return 0

    print(f"\nFitting ({args.objective}, {args.restarts} restarts) ...")
    result = fit(rows, objective=args.objective, restarts=args.restarts,
                 seed=args.seed, fixed_penalty=args.fix_penalty,
                 regularization=args.regularization)
    fitted = build_cfg(result["weights"],
                       {"external_dependency_detected": -result["penalty"]},
                       result["threshold"])

    # Signals with no data in this dataset keep whatever they had, so fitting
    # on a dataset that happens to lack history never silently zeroes it.
    for name, weight in current["signal_weights"].items():
        if name not in fitted["signal_weights"]:
            fitted["signal_weights"][name] = weight

    before = print_report(rows, current, f"BEFORE -- current ({args.weights})",
                          per_row=False)
    after = print_report(rows, fitted, "AFTER -- fitted")

    cv = None
    if not args.no_cv:
        print("\nCross-validation:")
        cv = cross_validate(rows, args.objective, seed=args.seed,
                            fixed_penalty=args.fix_penalty,
                            regularization=args.regularization)
        if cv:
            print(f"  mean {args.objective} = {cv['mean']:.3f} +/- {cv['std']:.3f} "
                  f"over {cv['folds']} folds")

    print("\nDelta: "
          f"accuracy {before['accuracy']:.3f} -> {after['accuracy']:.3f}, "
          f"f1 {before['f1']:.3f} -> {after['f1']:.3f}, "
          f"precision {before['precision']:.3f} -> {after['precision']:.3f}, "
          f"recall {before['recall']:.3f} -> {after['recall']:.3f}")

    if args.command == "compare":
        print("\n(compare only -- nothing written)")
        return 0

    fitted["metadata"] = {
        "fitted_at": datetime.now(timezone.utc).isoformat(),
        "source": args.source,
        "n_samples": len(rows),
        "n_positive": sum(r["label"] for r in rows),
        "objective": args.objective,
        "regularization": args.regularization,
        "train_metrics": {k: round(v, 4) for k, v in after.items()},
        "cv_mean": round(cv["mean"], 4) if cv else None,
        "cv_std": round(cv["std"], 4) if cv else None,
        "previous_metrics": {k: round(v, 4) for k, v in before.items()},
    }
    fitted["notes"] = current.get("notes", {})

    if not args.apply:
        print("\n--- fitted config (not written; re-run with --apply) ---")
        print(json.dumps(fitted, indent=2))
        return 0

    if after["accuracy"] < before["accuracy"] and after["f1"] <= before["f1"]:
        print("\nREFUSING to apply: the fitted weights are not better than the "
              "current ones on this data. Re-run with a different --objective "
              "or more data.")
        return 1

    confidence.save_weights(fitted, args.weights)
    print(f"\nWrote {args.weights}.")
    print("Next: `python run_benchmark_local.py` to confirm the benchmark still "
          "passes, then redeploy the processor so the worker picks it up.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
