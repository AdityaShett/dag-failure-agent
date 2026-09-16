#!/usr/bin/env python3
"""
export_results.py

Builds results/results.json -- the file dashboard.html loads -- from whatever
real sources are available:

  gcs       the per-run objects results_log.py writes (gs://$OUTCOMES_BUCKET/results/*.json)
  bigquery  confidence_signals joined to the latest confidence_outcomes row
  manifest  results/run_manifest.json, to fill in scenario metadata and to
            show published runs that never produced a result at all

The manifest join is the useful part: a run that was published but has no
result row is a run that vanished (crashed, was nacked, or was deduped), and
that used to be invisible. Those come through with actual_outcome="NO_RESULT"
so the dashboard's "unexpected outcome" counter catches them.

Usage:
    python export_results.py                        # gcs + manifest
    python export_results.py --source bigquery
    python export_results.py --source all --out results/results.json
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def load_manifest(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    raw = json.loads(p.read_text(encoding="utf-8"))
    rows = raw["runs"] if isinstance(raw, dict) else raw
    out = {}
    for row in rows:
        if row.get("run_id"):
            # The manifest carries the whole log body; drop it, it's large
            # and the dashboard has no use for it.
            out[row["run_id"]] = {k: v for k, v in row.items()
                                  if k != "synthetic_task_logs"}
    return out


def load_from_gcs(bucket_name: str, prefix: str) -> list:
    from google.cloud import storage

    client = storage.Client()
    rows = []
    for blob in client.bucket(bucket_name).list_blobs(prefix=f"{prefix}/"):
        if not blob.name.endswith(".json"):
            continue
        try:
            rows.append(json.loads(blob.download_as_text()))
        except Exception as e:
            print(f"  skipping {blob.name}: {e!r}")
    return rows


def load_from_bigquery(project: str, dataset: str, limit: int = 500) -> list:
    from google.cloud import bigquery

    sql = f"""
        WITH latest_outcome AS (
            SELECT record_id, outcome, pr_number, diff_applied, fallback_reason, updated_at,
                   ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY updated_at DESC) AS rn
            FROM `{project}.{dataset}.confidence_outcomes`
        )
        SELECT s.record_id, s.run_id, s.scenario_id, s.dag_id, s.task_id,
               s.confidence_score, s.confidence_tier, s.created_at,
               s.s_stack_trace_present, s.s_line_number_matches_source,
               s.s_known_fix_pattern_match, s.s_log_completeness,
               s.s_history_merge_rate, s.s_retrieval_support,
               s.s_external_dependency_detected,
               o.outcome, o.pr_number, o.diff_applied, o.fallback_reason
        FROM `{project}.{dataset}.confidence_signals` s
        LEFT JOIN latest_outcome o ON o.record_id = s.record_id AND o.rn = 1
        ORDER BY s.created_at DESC
        LIMIT {int(limit)}
    """
    client = bigquery.Client(project=project)
    rows = []
    for r in client.query(sql).result():
        signals = {
            name: r.get(f"s_{name}") for name in (
                "stack_trace_present", "line_number_matches_source",
                "known_fix_pattern_match", "log_completeness",
                "history_merge_rate", "retrieval_support",
                "external_dependency_detected",
            )
        }
        outcome = r["outcome"]
        rows.append({
            "run_id": r["run_id"],
            "scenario_id": r["scenario_id"],
            "dag_id": r["dag_id"],
            "task_id": r["task_id"],
            "confidence_score": r["confidence_score"],
            "confidence_tier": r["confidence_tier"],
            "signals": signals,
            "actual_outcome": "PR_CREATED" if r["pr_number"] else "NO_CONFIDENT_FIX",
            "pr_number": r["pr_number"],
            "diff_applied": r["diff_applied"],
            "fallback_reason": r["fallback_reason"],
            "bq_outcome": outcome,
            "mode": "agent-run",
            "timestamp": r["created_at"].isoformat() if r["created_at"] else None,
            "confidence_record_id": r["record_id"],
        })
    return rows


def merge(rows: list, manifest: dict, github_repo: str) -> list:
    """Manifest fields fill gaps in a result row; they never overwrite
    something the agent actually reported."""
    merged = []
    seen = set()
    for row in rows:
        run_id = row.get("run_id")
        seen.add(run_id)
        base = dict(manifest.get(run_id, {}))
        base.update({k: v for k, v in row.items() if v is not None})
        if base.get("pr_number") and not base.get("pr_url") and github_repo:
            base["pr_url"] = f"https://github.com/{github_repo}/pull/{base['pr_number']}"
        merged.append(base)

    for run_id, entry in manifest.items():
        if run_id in seen:
            continue
        merged.append({
            **entry,
            "actual_outcome": "NO_RESULT",
            "confidence_score": None,
            "mode": "published-no-result",
            "note": "published to Pub/Sub but no result row was ever written "
                    "(worker crash, dedupe skip, or message never delivered)",
        })

    merged.sort(key=lambda r: str(r.get("timestamp") or ""), reverse=True)
    return merged


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", default="gcs",
                        choices=["gcs", "bigquery", "all"])
    parser.add_argument("--bucket", default=os.environ.get("OUTCOMES_BUCKET"))
    parser.add_argument("--prefix", default=os.environ.get("RESULTS_PREFIX", "results"))
    parser.add_argument("--project", default=os.environ.get("GCP_PROJECT"))
    parser.add_argument("--dataset", default=os.environ.get("BQ_DATASET", "dag_failure_agent"))
    parser.add_argument("--manifest", default="results/run_manifest.json")
    parser.add_argument("--scenarios", default="config/scenarios.json")
    parser.add_argument("--weights", default="config/weights.json")
    parser.add_argument("--out", default="results/results.json")
    args = parser.parse_args()

    rows = []
    if args.source in ("gcs", "all"):
        if not args.bucket:
            print("  no --bucket / OUTCOMES_BUCKET set; skipping GCS")
        else:
            gcs_rows = load_from_gcs(args.bucket, args.prefix)
            print(f"  {len(gcs_rows)} row(s) from gs://{args.bucket}/{args.prefix}")
            rows += gcs_rows
    if args.source in ("bigquery", "all"):
        if not args.project:
            print("  no --project / GCP_PROJECT set; skipping BigQuery")
        else:
            bq_rows = load_from_bigquery(args.project, args.dataset)
            print(f"  {len(bq_rows)} row(s) from BigQuery")
            rows += bq_rows

    github_repo = ""
    scenarios_path = Path(args.scenarios)
    if scenarios_path.exists():
        github_repo = json.loads(scenarios_path.read_text(encoding="utf-8")).get(
            "github_repo", "")

    manifest = load_manifest(args.manifest)
    print(f"  {len(manifest)} published run(s) in {args.manifest}")

    merged = merge(rows, manifest, github_repo)

    weights = None
    weights_path = Path(args.weights)
    if weights_path.exists():
        weights = json.loads(weights_path.read_text(encoding="utf-8"))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "agent-run",
        "source": args.source,
        "weights": weights,
        "runs": merged,
    }, indent=2) + "\n", encoding="utf-8")

    unexpected = sum(1 for r in merged
                     if r.get("expected_outcome")
                     and r.get("actual_outcome") != r["expected_outcome"])
    print(f"\nWrote {len(merged)} run(s) to {out} ({unexpected} unexpected outcome(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
