"""
results_log.py -- append one row per completed run, in the schema the
dashboard reads.

Why this is not "pr.py writes results/results.json": the worker runs in
Cloud Run, on an ephemeral read-only-ish filesystem, across many container
instances. A file written inside the container is gone the moment the
instance is recycled and is invisible to everything else. So each run is
written as its own small object in GCS (results/<run_id>.json), and
export_results.py collates those into results/results.json for the
dashboard. One object per run also means concurrent workers can't clobber
each other, which a single shared JSON array would.

If OUTCOMES_BUCKET isn't set (local dev), it falls back to appending to a
local JSONL file so the same code path still produces something you can
look at.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

_client = None

RESULTS_PREFIX = os.environ.get("RESULTS_PREFIX", "results")
LOCAL_RESULTS_PATH = os.environ.get("LOCAL_RESULTS_PATH", "results/runs.jsonl")


def _get_client():
    global _client
    if _client is None:
        from google.cloud import storage as gcs_storage
        _client = gcs_storage.Client()
    return _client


def build_row(state: dict, actual_outcome: str) -> dict:
    """The schema the dashboard expects. Keep this and dashboard.html in
    step -- run_benchmark_local.py and export_results.py emit the same keys."""
    return {
        "run_id": state.get("run_id"),
        "scenario_id": state.get("scenario_id"),
        "dag_id": state.get("dag_id"),
        "task_id": state.get("task_id"),
        "difficulty": state.get("difficulty"),
        "failure_type": state.get("failure_type"),
        "expected_outcome": state.get("expected_outcome"),
        "actual_outcome": actual_outcome,
        "confidence_score": state.get("confidence_score"),
        "confidence_tier": state.get("confidence_tier"),
        "confidence_threshold": state.get("confidence_threshold"),
        "signals": state.get("confidence_signals"),
        "contributions": state.get("confidence_contributions"),
        "gate_reason": state.get("gate_reason"),
        "diff_applied": state.get("diff_applied"),
        "source_ref": state.get("source_ref"),
        "pr_url": state.get("pr_url"),
        "confidence_record_id": state.get("confidence_record_id"),
        "mode": "agent-run",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def record_run(state: dict, actual_outcome: str) -> dict:
    """Best-effort. A failure to log a result must never fail a run that
    otherwise succeeded, so everything here is caught and printed."""
    row = build_row(state, actual_outcome)
    bucket_name = os.environ.get("OUTCOMES_BUCKET")

    if not bucket_name:
        try:
            os.makedirs(os.path.dirname(LOCAL_RESULTS_PATH) or ".", exist_ok=True)
            with open(LOCAL_RESULTS_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps(row) + "\n")
        except Exception as e:
            print(f"WARNING: could not append local result row: {e!r}")
        return row

    try:
        run_id = row.get("run_id") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in str(run_id))
        blob = _get_client().bucket(bucket_name).blob(f"{RESULTS_PREFIX}/{safe}.json")
        blob.upload_from_string(json.dumps(row), content_type="application/json")
    except Exception as e:
        print(f"WARNING: could not write result row to gs://{bucket_name}: {e!r}")

    return row
