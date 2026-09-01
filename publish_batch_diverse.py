import os, time
from publish_test_message import publish

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Original baseline scenarios — kept as-is. In test run #1 these landed
# almost entirely "medium" tier or fallback.
BASELINE_SCENARIOS = [
    ("dag1", "load_dataset",         "log_typo_short.txt"),
    ("dag2", "merge_partitions",     "log_dependency_long.txt"),
    ("dag3", "normalize_records",    "log_import_error.txt"),
    ("dag4", "enrich_with_scores",   "log_null_ambiguous.txt"),
    ("dag5", "write_report_to_gcs",  "log_permission_short.txt"),
    ("dag6", "call_partner_api",     "log_timeout_long.txt"),
]

# New: unambiguous root cause + seeded merged precedent + seeded retrieval
# match -> designed to push confidence_score >= 0.75 (HIGH tier).
# Requires seed_history.py to have been run first with matching task_ids.
EASY_SCENARIOS = [
    ("dag1", "load_dataset_easy",       "log_easy_load.txt"),
    ("dag2", "merge_partitions_easy",   "log_easy_merge.txt"),
    ("dag3", "normalize_records_easy",  "log_easy_normalize.txt"),
]

# New: genuinely ambiguous/contradictory logs + seeded mostly-rejected
# history + no retrieval precedent -> designed to push confidence_score
# below 0.50 (LOW tier).
HARD_SCENARIOS = [
    ("dag4", "enrich_with_scores_hard",   "log_hard_enrich.txt"),
    ("dag5", "write_report_to_gcs_hard",  "log_hard_report.txt"),
    ("dag6", "call_partner_api_hard",     "log_hard_partner.txt"),
]

REPEATS_PER_SCENARIO = 4

ALL_SCENARIOS = BASELINE_SCENARIOS + EASY_SCENARIOS + HARD_SCENARIOS

for dag_id, task_id, log_file in ALL_SCENARIOS:
    log_path = os.path.join(SCRIPT_DIR, "weight_tests", log_file)
    with open(log_path) as f:
        logs = f.read()
    for i in range(REPEATS_PER_SCENARIO):
        publish(
            dag_id, task_id, f"tests/{dag_id}.py", logs,
            github_repo="AdityaShett/dag-failure-agent",
        )
        time.sleep(2)

total = len(ALL_SCENARIOS) * REPEATS_PER_SCENARIO
print(
    f"Published {total} runs "
    f"({len(BASELINE_SCENARIOS)} baseline + {len(EASY_SCENARIOS)} easy + "
    f"{len(HARD_SCENARIOS)} hard) x{REPEATS_PER_SCENARIO} repeats"
)
