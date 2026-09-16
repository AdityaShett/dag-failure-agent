# Weight tuning + dashboard — what changed and how to ship it

## 1. The short version

Weight tuning wasn't tunable, for three independent reasons:

1. **`config/weights.json` was never read.** It used the `signal_weights` shape;
   `nodes.py`'s loader required `history`/`logs`/`source`. Every cold start took the
   `missing keys` branch and silently used hardcoded defaults.
2. **The signals were constants.** `s_logs = 1.0 if len(logs) > 50`,
   `s_source = 1.0 if len(source) > 50`. Both are 1.0 on any real run, so the weighted
   mean was always 1.0. That's the `confidence_score: 1.0` that contradicted the LLM's own
   "Medium" on the dag2 run. No weighting of two constants produces a different answer.
3. **The threshold decided nothing.** `route_after_fix` branched only on whether
   `proposed_fix` started with `NO_CONFIDENT_FIX`. `confidence_threshold` was logged and
   printed in PR bodies, never compared to anything.

All three are fixed. Scoring now lives in one shared module, the signals are real, and the
threshold gates PR creation.

## 2. What the score is now

```
score = clamp( Σ(wᵢ · sᵢ) / Σ(wᵢ)  −  penalty ,  0, 1 )
```

| Signal | What it measures |
|---|---|
| `stack_trace_present` | 1.0 full traceback + exception · 0.8 frames, no header · 0.4 exception only · 0.0 nothing |
| `line_number_matches_source` | **1.0** failing line is where the log says · **0.6** exists elsewhere in the file · **0.25** only the function matches · **0.0** not in this file at all |
| `known_fix_pattern_match` | Exception type has a mechanical fix template |
| `log_completeness` | Timestamps, levels, ≥4 lines, dag/task named, ≥200 chars |
| `history_merge_rate` | *optional* — past agent PRs merged for this dag+task |
| `retrieval_support` | *optional* — similar prior incidents found |
| `external_dependency_detected` | **penalty** — failure bottoms out in a third-party call |

`line_number_matches_source = 0.0` is the guard for your stale-DAG problem: a real traceback
analysed against a file that no longer contains the failing line can no longer clear the
threshold. Optional signals that are absent are **dropped and the mean renormalised** — never
guessed at 0.5, which is what the old code did.

## 3. Files

**New**
| File | Purpose |
|---|---|
| `processor/agent/confidence.py` | All signal extraction + scoring. Stdlib only. Shared by worker, benchmark, tuner, dashboard. |
| `processor/agent/results_log.py` | Writes one result object per run to GCS (`results/<run_id>.json`). |
| `run_benchmark_local.py` | Scores all 6 scenarios offline in ~50 ms. No GCP. |
| `tune_weights.py` | `score` / `fit` / `compare`. Replaces all four old tuners. |
| `export_results.py` | Collates GCS + BigQuery + manifest → `results/results.json`. |
| `config/tuning_fixtures.json` | 17 labelled rows so tuning is runnable today, without GCP. |
| `migrations/001_*.sql` | Adds the real per-signal columns to `confidence_signals`. |
| `tests/test_confidence.py` | 17 assertions locking in every bug above. |

**Rewritten** — `config/weights.json` (canonical v2 shape), `dashboard.html`,
`dashboard/app.py`, `Dockerfile.dashboard`, `confidence_signals_schema.json`

**Modified** — `nodes.py` (shared scorer, real signals, fixed `fetch_task_logs` call, all
signals logged to BQ), `graph.py` (threshold now gates), `tools/context.py`
(`build_task_log_filter`, `fetch_dag_source(..., ref=)`), `processor_app.py` (graph wrapped
in try/except, `/weights` endpoint, result rows), `pr.py` (honours the publisher's
`branch_name`), `state.py`, `publish_batch_diverse.py` (`--source-ref`, `failure_type`)

**Delete** — `tune_weights_3signal.py`, `score_manual_weights.py` (both superseded; they
wrote a `weights.json` shape `nodes.py` couldn't read)

## 4. Other bugs fixed along the way

- **`fetch_task_logs()` TypeError** — `collect_context` called it with 3 positional args; it
  takes one filter string. New `build_task_log_filter(dag_id, task_id, run_id)` builds it,
  time-windowed on the run_id.
- **`fetch_dag_source` reading `main`** — now takes `ref`, from the payload or `DAG_SOURCE_REF`.
  This unblocks you *without* merging `test/verify-secret-fix` first.
- **Poison-pill retries** — `agent_graph.invoke` is wrapped; failures return 200 (ack). This is
  what turned one bad message into 1.5 hours of PR spam.
- **Branch names** — `pr.py` now uses the publisher's `agent/fix-<dag>-<run_id>`, so a PR is
  traceable to the run that made it.
- **False-positive external detection** — a `KeyError` raised inside pandas was initially
  flagged as an external dependency and sank dag1. The marker list is now network clients only.

## 5. Implement + test

Everything in steps 1–3 runs locally with **no GCP credentials**.

### Step 1 — copy files in, run the tests
```bash
python tests/test_confidence.py        # expect: all checks passed
python run_benchmark_local.py          # expect: 6/6 scenarios matched
```
`run_benchmark_local.py` prints every signal per scenario, so a miss tells you *which* signal
moved.

### Step 2 — verify the weights actually bite
```bash
python tune_weights.py score           # 17/17 fixtures, TP=4 FP=0 TN=13 FN=0
python tune_weights.py compare         # fits a candidate, shows it side by side
python tune_weights.py fit --apply     # writes config/weights.json (refuses if worse)
```
Sanity check that tuning is real: set `line_number_matches_source` to `0.0` in
`config/weights.json` and re-run `tune_weights.py score` — the stale-source fixtures start
passing, i.e. FP appears. Put it back.

### Step 3 — the dashboard
Open `dashboard.html` in a browser. No server, no CDN, no build step.
- **Benchmark (6)** / **Fixtures (17)** toggle the embedded datasets
- Sliders re-score every run live (same formula as Python — verified identical to 3 decimals)
- **Auto-fit** runs the same search as `tune_weights.py` in-browser (~150 ms)
- **Show weights.json** prints the config to paste into `config/weights.json`
- **Load results.json** takes real output from `run_benchmark_local.py` or `export_results.py`

### Step 4 — deploy
```bash
bq query --use_legacy_sql=false < migrations/001_confidence_signals_add_signal_columns.sql

# point the worker at the unmerged branch until you merge it
gcloud run services update dag-failure-processor \
  --set-env-vars DAG_SOURCE_REF=test/verify-secret-fix

gcloud builds submit --config cloudbuild.processor.yaml
curl https://<processor-url>/weights     # confirms which weights the container loaded
```

### Step 5 — real batch
```bash
python publish_batch_diverse.py --project dag-failure-agent-505623 \
  --topic dag-failure-events --dry-run          # check payloads first
python publish_batch_diverse.py --project dag-failure-agent-505623 \
  --topic dag-failure-events --source-ref test/verify-secret-fix

python export_results.py --source all           # → results/results.json
```
Then load that `results.json` into `dashboard.html`. Runs that were published but produced no
result row show up as `NO_RESULT` rather than silently vanishing.

### Step 6 — deploy the Streamlit dashboard (optional)
```bash
gcloud builds submit --config cloudbuild.dashboard.yaml
```
`Dockerfile.dashboard` now also copies `processor/agent/confidence.py` and
`config/weights.json` — without those the Weights tab can't score anything.

## 6. Honest caveats

- **`run_benchmark_local.py` scores the confidence gate only.** No LLM call, no PR. It tells
  you what the weights would decide, which is the part the weights control. Rows are tagged
  `mode: local-scoring` so they can't be mistaken for end-to-end runs.
- **17 fixtures is a small dataset**, and 13 of the 17 negatives are legacy logs I labelled 0
  on the principle that a log describing code which no longer exists should be refused, not
  patched. That's a defensible label, not ground truth. Unregularised, the fitter collapses
  onto `line_number_matches_source` alone — perfect on 17 rows, brittle in production — so it
  now carries a concentration penalty. Re-tune with `--source bigquery` once you have ≥5
  human-labelled PRs per class.
- **The two-confidence-tiers question from your changelog is now answered**: there is one
  score, from one module, compared to one threshold. The LLM's prose "confidence (high/medium/low)"
  is still just text in the root-cause output — it's never parsed and never gates anything.
- **`history_merge_rate` and `retrieval_support` are untested against real data** — no history
  existed locally to exercise them. Their weights are small (0.07 / 0.05) for that reason.
