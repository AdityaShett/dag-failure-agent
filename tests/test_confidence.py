#!/usr/bin/env python3
"""
tests/test_confidence.py -- run with:  python -m pytest tests/test_confidence.py -q
                            or simply: python tests/test_confidence.py

No GCP, no network, no LLM. These lock in the behaviours that were actually
broken, so a regression shows up here rather than as a mystery PR in GitHub.
"""
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "processor"))

from agent import confidence  # noqa: E402

WEIGHTS = confidence.load_weights(str(ROOT / "config" / "weights.json"), strict=True)

DAG1_SOURCE = '''from datetime import datetime
import pandas as pd


def transform_data(**context):
    df = pd.DataFrame(context["ti"].xcom_pull(key="raw_df"))
    user_ids = df["user_id"].tolist()
    return user_ids
'''

DAG1_LOG = '''[2026-09-10 03:12:41,558] {taskinstance.py:1937} INFO - Starting attempt 1 of 1
[2026-09-10 03:12:41,559] {taskinstance.py:2170} INFO - Executing <Task(PythonOperator): transform_data>
[2026-09-10 03:12:41,812] {python.py:194} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/opt/airflow/dags/tests/dag1.py", line 7, in transform_data
    user_ids = df["user_id"].tolist()
  File "/usr/local/lib/python3.11/site-packages/pandas/core/frame.py", line 3893, in __getitem__
    indexer = self.columns.get_loc(key)
KeyError: 'user_id'
[2026-09-10 03:12:41,815] {taskinstance.py:2731} INFO - Marking task as FAILED. dag_id=dag1, task_id=transform_data
'''

EXTERNAL_LOG = '''[2026-09-10 03:20:44,110] {taskinstance.py:1937} INFO - Starting attempt 1 of 1
[2026-09-10 03:21:14,890] {python.py:194} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/opt/airflow/dags/tests/dag5.py", line 18, in call_partner_api
    response.raise_for_status()
  File "/usr/local/lib/python3.11/site-packages/requests/models.py", line 1024, in raise_for_status
    raise HTTPError(http_error_msg, response=self)
requests.exceptions.HTTPError: 500 Server Error: Internal Server Error for url: https://partner-api.example.com/v2/report
'''

NO_TRACE_LOG = '''[2026-09-10 03:23:59,004] {taskinstance.py:1937} INFO - Starting attempt 1 of 1
[2026-09-10 03:24:11,442] {local_task_job_runner.py:266} INFO - Task exited with return code 1
[2026-09-10 03:24:11,450] {taskinstance.py:2731} INFO - Marking task as FAILED. dag_id=dag6, task_id=run_batch_job
'''

failures = []


def check(name, condition, detail=""):
    if condition:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name} {detail}")
        failures.append(name)


def test_matching_log_and_source_scores_high():
    result = confidence.score_run(DAG1_LOG, DAG1_SOURCE, WEIGHTS, "dag1", "transform_data")
    check("matching log+source clears the threshold",
          result["meets_threshold"], f"score={result['score']}")
    check("line match is exact",
          result["signals"]["line_number_matches_source"] == 1.0)


def test_stale_source_is_caught():
    """The dag2 incident: a real traceback analysed against a file that no
    longer contains the failing line must NOT clear the threshold."""
    stale = "def something_else():\n    return 1\n"
    result = confidence.score_run(DAG1_LOG, stale, WEIGHTS, "dag1", "transform_data")
    check("stale source is gated",
          not result["meets_threshold"], f"score={result['score']}")
    check("line match is zero on stale source",
          result["signals"]["line_number_matches_source"] == 0.0)


def test_drifted_source_scores_between():
    """Same bug, different line number: partial credit, not zero."""
    drifted = "# a new comment\n" * 20 + DAG1_SOURCE
    signals = confidence.extract_signals(DAG1_LOG, drifted, "dag1", "transform_data")
    check("drifted source gets partial credit",
          signals["line_number_matches_source"] == 0.6,
          f"got {signals['line_number_matches_source']}")


def test_external_failure_is_penalised():
    source = ("import requests\n\n\ndef call_partner_api(**context):\n"
              "    response = requests.get('https://partner-api.example.com/v2/report')\n"
              "    response.raise_for_status()\n")
    result = confidence.score_run(EXTERNAL_LOG, source, WEIGHTS, "dag5", "call_partner_api")
    check("external outage is gated despite a perfect traceback",
          not result["meets_threshold"], f"score={result['score']}")
    check("external dependency detected",
          result["signals"]["external_dependency_detected"] >= 0.8)


def test_library_frame_is_not_external():
    """A KeyError raised inside pandas is still our bug. This was a real
    false positive: it sank dag1 until the marker list was narrowed to
    network/service clients."""
    signals = confidence.extract_signals(DAG1_LOG, DAG1_SOURCE, "dag1", "transform_data")
    check("pandas frame is not treated as an external dependency",
          signals["external_dependency_detected"] == 0.0)


def test_no_traceback_scores_low():
    result = confidence.score_run(NO_TRACE_LOG, DAG1_SOURCE, WEIGHTS, "dag6", "run_batch_job")
    check("log with no traceback is gated", not result["meets_threshold"])
    check("stack_trace_present is zero", result["signals"]["stack_trace_present"] == 0.0)


def test_optional_signals_are_dropped_not_guessed():
    without = confidence.score_run(DAG1_LOG, DAG1_SOURCE, WEIGHTS, "dag1", "transform_data")
    with_history = confidence.score_run(DAG1_LOG, DAG1_SOURCE, WEIGHTS, "dag1",
                                        "transform_data", history_merge_rate=1.0)
    check("missing history doesn't drag the score down",
          without["score"] == with_history["score"],
          f"{without['score']} vs {with_history['score']}")
    check("missing history is recorded as None",
          without["signals"]["history_merge_rate"] is None)


def test_weights_actually_change_the_decision():
    """The whole point. Under the old scorer every run was 1.0 and no weight
    change could move anything."""
    stale = "def something_else():\n    return 1\n"
    signals = confidence.extract_signals(DAG1_LOG, stale, "dag1", "transform_data")

    lenient = confidence.normalize_weights_config({
        "signal_weights": {"stack_trace_present": 0.5, "known_fix_pattern_match": 0.5,
                           "line_number_matches_source": 0.0},
        "penalty_weights": {"external_dependency_detected": -0.4},
        "confidence_threshold": 0.7,
    })
    strict = confidence.normalize_weights_config({
        "signal_weights": {"stack_trace_present": 0.1, "known_fix_pattern_match": 0.1,
                           "line_number_matches_source": 0.8},
        "penalty_weights": {"external_dependency_detected": -0.4},
        "confidence_threshold": 0.7,
    })
    check("lenient weights open a PR on stale source",
          confidence.score_signals(signals, lenient)["meets_threshold"])
    check("strict weights gate the same run",
          not confidence.score_signals(signals, strict)["meets_threshold"])


def test_legacy_weights_file_still_loads():
    """The old 3-signal shape must map onto the new signals rather than
    silently falling back to defaults, which is what hid the bug for so long."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "weights.json"
        path.write_text(json.dumps({"weights": {"history": 0.34, "logs": 0.33,
                                                "source": 0.33}}))
        cfg = confidence.load_weights(str(path), strict=True)
    check("legacy history/logs/source maps to v2 signals",
          "line_number_matches_source" in cfg["signal_weights"]
          and "log_completeness" in cfg["signal_weights"])


def test_negative_weight_in_signal_weights_is_treated_as_penalty():
    """The original config/weights.json put external_dependency_detected at
    -0.40 inside signal_weights. That shape must keep working."""
    cfg = confidence.normalize_weights_config({
        "signal_weights": {"stack_trace_present": 0.3,
                           "line_number_matches_source": 0.3,
                           "known_fix_pattern_match": 0.3,
                           "log_completeness": 0.1,
                           "external_dependency_detected": -0.40},
        "confidence_threshold": 0.70,
    })
    check("negative signal weight is relocated to penalty_weights",
          cfg["penalty_weights"].get("external_dependency_detected") == -0.4)


def test_benchmark_passes_end_to_end():
    """All six scenarios, scored off the real files, against the shipped
    weights."""
    scenarios = json.loads((ROOT / "config" / "scenarios.json").read_text())["scenarios"]
    wrong = []
    for scenario in scenarios:
        log = (ROOT / "weight_tests" / scenario["log_file"]).read_text(errors="replace")
        src_path = ROOT / scenario["code_file"]
        source = src_path.read_text(errors="replace") if src_path.exists() else ""
        result = confidence.score_run(log, source, WEIGHTS,
                                      scenario["dag_id"], scenario["task_id"])
        actual = "PR_CREATED" if result["meets_threshold"] else "NO_CONFIDENT_FIX"
        if actual != scenario["expected_outcome"]:
            wrong.append(f"{scenario['scenario_id']}={actual}@{result['score']}")
    check(f"all {len(scenarios)} benchmark scenarios match expectations",
          not wrong, "; ".join(wrong))


def main():
    print("confidence scoring tests\n")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print()
    if failures:
        print(f"{len(failures)} FAILED: {', '.join(failures)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
