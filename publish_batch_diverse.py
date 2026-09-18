
import argparse
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("publish_batch_diverse")


def load_scenarios(path: str) -> dict:
    with open(path, "r") as f:
        config = json.load(f)
    if "scenarios" not in config or not config["scenarios"]:
        raise ValueError(f"No scenarios found in {path}")
    return config


def make_run_id(scenario_id: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    short_uuid = uuid.uuid4().hex[:8]
    return f"run-{scenario_id}-{timestamp}-{short_uuid}"


def load_log_contents(log_file: str, weight_tests_dir: str) -> str:
    log_path = Path(weight_tests_dir) / log_file
    with open(log_path, "r") as f:
        return f.read()


def build_payload(scenario: dict, run_id: str, github_repo: str, weight_tests_dir: str,
                  source_ref: str = None) -> dict:
    return {
        "run_id": run_id,
        "github_repo": github_repo,
        "dag_id": scenario["dag_id"],
        "task_id": scenario["task_id"],
        "target_file": scenario["code_file"],
        "synthetic_task_logs": load_log_contents(scenario["log_file"], weight_tests_dir),
        "log_file": scenario["log_file"],
        "scenario_id": scenario["scenario_id"],
        "difficulty": scenario.get("difficulty"),
        "failure_type": scenario.get("failure_type"),
        "expected_outcome": scenario.get("expected_outcome"),
        "source_ref": source_ref,
        "branch_name": f"agent/fix-{scenario['dag_id']}-{run_id}",
    }


def publish_message(publisher, topic_path, payload: dict) -> str:
    """Publish a single JSON payload to Pub/Sub and return the message id."""
    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic_path, data=data, run_id=payload["run_id"])
    return future.result()


def main():
    parser = argparse.ArgumentParser(description="Publish diverse benchmark scenarios to Pub/Sub")
    parser.add_argument("--project", required=True, help="GCP project id")
    parser.add_argument("--topic", required=True, help="Pub/Sub topic name")
    parser.add_argument(
        "--scenarios",
        default="config/scenarios.json",
        help="Path to scenarios.json",
    )
    parser.add_argument(
        "--weight-tests-dir",
        default="weight_tests",
        help="Directory containing the log_*.txt files referenced by scenarios.json",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=5.0,
        help="Delay between publishing each scenario (default: 5s)",
    )
    parser.add_argument(
        "--manifest-out",
        default="results/run_manifest.json",
        help="Where to write the manifest of published run_ids (for the dashboard)",
    )
    parser.add_argument(
        "--source-ref",
        default=None,
        help="Git ref the worker should read DAG source from (e.g. a feature "
             "branch that isn't merged yet). Defaults to the repo's default branch.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build payloads and write the manifest without calling Pub/Sub",
    )
    args = parser.parse_args()

    config = load_scenarios(args.scenarios)
    github_repo = config.get("github_repo", "")
    scenarios = config["scenarios"]
    repeats = config.get("repeats_per_scenario", 1)

    publisher = None
    topic_path = None
    if not args.dry_run:
        from google.cloud import pubsub_v1

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(args.project, args.topic)

    manifest = []
    total = len(scenarios) * repeats
    log.info(f"Publishing {total} run(s) across {len(scenarios)} scenario(s), "
              f"{args.delay_seconds}s delay between runs, dry_run={args.dry_run}")

    count = 0
    for scenario in scenarios:
        for repeat_index in range(repeats):
            run_id = make_run_id(scenario["scenario_id"])
            payload = build_payload(scenario, run_id, github_repo,
                                    args.weight_tests_dir, args.source_ref)
            count += 1

            log.info(
                f"[{count}/{total}] scenario={scenario['scenario_id']} "
                f"difficulty={scenario.get('difficulty')} "
                f"dag_id={scenario['dag_id']} log_file={scenario['log_file']} "
                f"run_id={run_id} expected={scenario.get('expected_outcome')}"
            )

            if args.dry_run:
                message_id = "dry-run"
            else:
                message_id = publish_message(publisher, topic_path, payload)
                log.info(f"  -> published Pub/Sub message_id={message_id}")

            manifest.append({**payload, "message_id": message_id})

            if count < total:
                time.sleep(args.delay_seconds)

    manifest_path = Path(args.manifest_out)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    log.info(f"Wrote manifest of {len(manifest)} run(s) to {manifest_path}")


if __name__ == "__main__":
    main()
