#!/usr/bin/env python3
"""
run_benchmark_local.py

Scores every scenario in config/scenarios.json using the *same* signal
extraction and weighting the Cloud Run worker uses
(processor/agent/confidence.py), and writes results/results.json.

Why this exists: until now the only way to find out whether a weight change
helped was to publish 6 Pub/Sub messages, wait for Cloud Run, wait for
Gemini, and read PR titles. That loop is minutes long, costs money, and
mixes up three different failure modes (publisher bug, stale source, bad
weights). This runs the confidence half of the pipeline in about 50ms with
no GCP credentials, so weight tuning is a tight loop again.

What it does NOT do: call the LLM or open PRs. It reports what the
confidence gate would decide -- PR_CREATED if score >= confidence_threshold,
NO_CONFIDENT_FIX otherwise -- which is exactly the decision the weights
control. Every row it writes is tagged mode="local-scoring" so it can never
be mistaken for a real end-to-end run in the dashboard.

Usage:
    python run_benchmark_local.py
    python run_benchmark_local.py --weights config/weights.json --out results/results.json
    python run_benchmark_local.py --source-from-github --ref test/verify-secret-fix
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "processor"))

from agent import confidence  # noqa: E402


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def load_source_local(code_file: str, repo_root: Path) -> str:
    path = repo_root / code_file
    if not path.exists():
        print(f"  WARNING: {code_file} not found locally -- scoring with empty source")
        return ""
    return read_text(path)


def load_source_github(code_file: str, repo: str, ref: str) -> str:
    """Mirrors what the worker's fetch_dag_source does, so a local benchmark
    can reproduce the 'worker is reading main, publisher is sending the
    feature branch' mismatch instead of hiding it."""
    from github import Auth, Github  # imported lazily: only needed with --source-from-github

    token = os.environ.get("GITHUB_TOKEN")
    gh = Github(auth=Auth.Token(token)) if token else Github()
    contents = gh.get_repo(repo).get_contents(code_file, ref=ref)
    return contents.decoded_content.decode("utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenarios", default="config/scenarios.json")
    parser.add_argument("--weights", default="config/weights.json")
    parser.add_argument("--weight-tests-dir", default="weight_tests")
    parser.add_argument("--repo-root", default=".",
                        help="Root to resolve scenario code_file paths against")
    parser.add_argument("--out", default="results/results.json")
    parser.add_argument("--source-from-github", action="store_true",
                        help="Fetch DAG source from GitHub instead of the local checkout")
    parser.add_argument("--ref", default=None,
                        help="Git ref to fetch source from (default: repo default branch)")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    weights_cfg = confidence.load_weights(args.weights, strict=True)
    threshold = weights_cfg["confidence_threshold"]

    config = json.loads(read_text(Path(args.scenarios)))
    scenarios = config["scenarios"]
    github_repo = config.get("github_repo", "")

    rows = []
    ok = 0
    for scenario in scenarios:
        log_path = Path(args.weight_tests_dir) / scenario["log_file"]
        task_logs = read_text(log_path)

        if args.source_from_github:
            dag_source = load_source_github(
                scenario["code_file"], github_repo, args.ref or "HEAD")
        else:
            dag_source = load_source_local(scenario["code_file"], repo_root)

        result = confidence.score_run(
            task_logs=task_logs,
            dag_source=dag_source,
            weights_cfg=weights_cfg,
            dag_id=scenario["dag_id"],
            task_id=scenario["task_id"],
        )

        actual = "PR_CREATED" if result["meets_threshold"] else "NO_CONFIDENT_FIX"
        expected = scenario.get("expected_outcome")
        matched = actual == expected
        ok += matched

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        rows.append({
            "run_id": f"local-{scenario['scenario_id']}-{stamp}",
            "scenario_id": scenario["scenario_id"],
            "dag_id": scenario["dag_id"],
            "task_id": scenario["task_id"],
            "difficulty": scenario.get("difficulty"),
            "failure_type": scenario.get("failure_type"),
            "expected_outcome": expected,
            "actual_outcome": actual,
            "confidence_score": result["score"],
            "confidence_tier": result["tier"],
            "signals": result["signals"],
            "contributions": result["contributions"],
            "pr_url": None,
            "mode": "local-scoring",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        if not args.quiet:
            mark = "OK " if matched else "MISS"
            print(f"[{mark}] {scenario['scenario_id']:<22} score={result['score']:.3f} "
                  f"({result['tier']:<6}) expected={expected:<16} got={actual}")
            for name, value in result["signals"].items():
                shown = "n/a" if value is None else f"{value:.2f}"
                print(f"         {name:<28} {shown}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "local-scoring",
        "weights": weights_cfg,
        "runs": rows,
    }, indent=2) + "\n", encoding="utf-8")

    print(f"\n{ok}/{len(rows)} scenarios matched their expected outcome "
          f"(threshold={threshold})")
    print(f"Wrote {out_path}")
    return 0 if ok == len(rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
