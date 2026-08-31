import os, random, time
from publish_test_message import publish

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

SCENARIOS = [
    ("dag1", "load_dataset",         "log_typo_short.txt"),
    ("dag2", "merge_partitions",     "log_dependency_long.txt"),
    ("dag3", "normalize_records",    "log_import_error.txt"),
    ("dag4", "enrich_with_scores",   "log_null_ambiguous.txt"),
    ("dag5", "write_report_to_gcs",  "log_permission_short.txt"),
    ("dag6", "call_partner_api",     "log_timeout_long.txt"),
]

REPEATS_PER_SCENARIO = 4  # 6 scenarios x 4 = 24 runs

for dag_id, task_id, log_file in SCENARIOS:
    log_path = os.path.join(SCRIPT_DIR, "weight_tests", log_file)
    with open(log_path) as f:
        logs = f.read()
    for i in range(REPEATS_PER_SCENARIO):
        publish(
            dag_id,
            task_id,
            f"tests/{dag_id}.py",
            logs,
            github_repo="AdityaShett/dag-failure-agent",
        )
        time.sleep(2)