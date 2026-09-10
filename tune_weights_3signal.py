#!/usr/bin/env python3
"""
tune_weights_3signal.py

Loads confidence_signals + confidence_outcomes from BigQuery, fits weights
for the 3-signal formula (history, logs, source), and writes the result to
config/weights.json for nodes.py to pick up.

Only run this AFTER confirming Query 1 from the guide returns real
(non-NULL) avg_s_history / avg_s_logs / avg_s_source values. If it's still
NULL, the record_id join is broken and this script will just exit with an
error telling you so.

Usage:
    python tune_weights_3signal.py
"""
import os
import json
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from sklearn.model_selection import train_test_split, KFold
from google.cloud import bigquery

PROJECT = os.environ.get("GCP_PROJECT", "dag-failure-agent-505623")
DATASET = os.environ.get("BQ_DATASET", "dag_failure_agent")
MIN_PER_CLASS = 5
OUTPUT_PATH = os.path.join("config", "weights.json")

SIGNAL_COLS = ["s_history", "s_logs", "s_source"]


def load_data(project: str) -> pd.DataFrame:
    query = f"""
        SELECT
            cs.record_id,
            cs.s_history,
            cs.s_logs,
            cs.s_source,
            co.outcome,
            CASE WHEN co.outcome = 'merged' THEN 1 ELSE 0 END AS merged
        FROM `{project}.{DATASET}.confidence_outcomes` co
        LEFT JOIN `{project}.{DATASET}.confidence_signals` cs
            ON co.record_id = cs.record_id
        WHERE co.outcome IN ('merged', 'rejected')
    """
    client = bigquery.Client(project=project)
    return client.query(query).to_dataframe()


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.dropna(subset=SIGNAL_COLS)
    dropped = before - len(df)
    if dropped:
        print(
            f"WARNING: dropped {dropped}/{before} rows with NULL signals "
            f"(record_id join didn't match for these — see the join-fix "
            f"diagnostics before trusting this run)."
        )

    n_merged = int((df["merged"] == 1).sum())
    n_rejected = int((df["merged"] == 0).sum())
    print(f"Usable rows: {len(df)} (merged={n_merged}, rejected={n_rejected})")

    if n_merged < MIN_PER_CLASS or n_rejected < MIN_PER_CLASS:
        print(
            f"ERROR: need at least {MIN_PER_CLASS} of each class with "
            f"non-null signals. Have merged={n_merged}, rejected={n_rejected}. "
            "Fix the record_id join or label more PRs before tuning."
        )
        sys.exit(1)

    return df


def compute_score(weights: np.ndarray, signals: pd.DataFrame) -> np.ndarray:
    return (
        weights[0] * signals["s_history"].values
        + weights[1] * signals["s_logs"].values
        + weights[2] * signals["s_source"].values
    )


def accuracy(weights: np.ndarray, signals: pd.DataFrame, outcomes: np.ndarray) -> float:
    scores = compute_score(weights, signals)
    predictions = (scores >= 0.5).astype(int)
    return (predictions == outcomes).sum() / len(outcomes)


def objective(weights: np.ndarray, signals: pd.DataFrame, outcomes: np.ndarray) -> float:
    return -accuracy(weights, signals, outcomes)


def fit(signals: pd.DataFrame, outcomes: np.ndarray) -> np.ndarray:
    x0 = np.array([0.34, 0.33, 0.33])
    bounds = [(0, 1), (0, 1), (0, 1)]
    constraints = {"type": "eq", "fun": lambda x: x.sum() - 1.0}
    result = minimize(
        objective, x0, args=(signals, outcomes),
        method="SLSQP", bounds=bounds, constraints=constraints,
    )
    return result.x


def tune(df: pd.DataFrame) -> dict:
    n = len(df)
    signals = df[SIGNAL_COLS]
    outcomes = df["merged"].values
    test_acc = None
    cv_mean = cv_std = None

    # With ~13 rows a held-out 20% split leaves 2-3 rows to test on, which
    # is not meaningful. Only split once there's enough to bother.
    if n >= 20:
        train, test = train_test_split(
            df, test_size=0.2, random_state=42, stratify=df["merged"]
        )
        train_signals, train_outcomes = train[SIGNAL_COLS], train["merged"].values
        test_signals, test_outcomes = test[SIGNAL_COLS], test["merged"].values

        best_weights = fit(train_signals, train_outcomes)
        train_acc = accuracy(best_weights, train_signals, train_outcomes)
        test_acc = accuracy(best_weights, test_signals, test_outcomes)
        print(f"Train accuracy: {train_acc:.3f} (n={len(train)})")
        print(f"Test accuracy:  {test_acc:.3f} (n={len(test)})")
    else:
        print(
            f"Only {n} rows total -- too few for a train/test split. "
            "Fitting on all data. Treat this result as provisional and "
            "re-tune once you have 20+ labeled examples per class."
        )
        best_weights = fit(signals, outcomes)
        train_acc = accuracy(best_weights, signals, outcomes)
        print(f"Full-data accuracy: {train_acc:.3f} (n={n})")

    # K-fold CV needs enough rows per fold to mean anything; skip below 10.
    if n >= 10:
        k = min(5, n)
        kf = KFold(n_splits=k, shuffle=True, random_state=42)
        cv_accs = []
        for fold_idx, (tr_idx, te_idx) in enumerate(kf.split(df)):
            tr, te = df.iloc[tr_idx], df.iloc[te_idx]
            w = fit(tr[SIGNAL_COLS], tr["merged"].values)
            acc = accuracy(w, te[SIGNAL_COLS], te["merged"].values)
            cv_accs.append(acc)
            print(f"  Fold {fold_idx + 1}: {acc:.3f}")
        cv_mean, cv_std = float(np.mean(cv_accs)), float(np.std(cv_accs))
        print(f"Mean CV accuracy: {cv_mean:.3f} +/- {cv_std:.3f}")
    else:
        print("Skipping cross-validation: fewer than 10 total rows.")

    return {
        "weights": {
            "history": round(float(best_weights[0]), 4),
            "logs": round(float(best_weights[1]), 4),
            "source": round(float(best_weights[2]), 4),
        },
        "n_samples": n,
        "train_accuracy": round(float(train_acc), 4),
        "test_accuracy": round(float(test_acc), 4) if test_acc is not None else None,
        "cv_mean_accuracy": round(cv_mean, 4) if cv_mean is not None else None,
        "cv_std_accuracy": round(cv_std, 4) if cv_std is not None else None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    print(f"Loading data from {PROJECT}.{DATASET} ...")
    df = load_data(PROJECT)
    df = validate_data(df)

    result = tune(df)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nWrote weights to {OUTPUT_PATH}:")
    print(json.dumps(result["weights"], indent=2))
    print(
        "\nCommit config/weights.json and redeploy dag-failure-processor "
        "for nodes.py to pick this up."
    )


if __name__ == "__main__":
    main()