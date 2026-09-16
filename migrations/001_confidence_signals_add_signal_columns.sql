-- 001_confidence_signals_add_signal_columns.sql
--
-- Adds the real per-signal columns to confidence_signals. The old columns
-- (s_llm, s_retrieval, s_history, s_logs, s_source) are left in place and
-- nullable so nothing that already reads them breaks; they are simply no
-- longer written to. s_logs/s_source in particular were the two degenerate
-- "is this string longer than 50 characters" signals, so historical rows of
-- them are near-constant and are not worth tuning against.
--
-- Run once:
--   bq query --use_legacy_sql=false < migrations/001_confidence_signals_add_signal_columns.sql

ALTER TABLE `dag_failure_agent.confidence_signals`
  ADD COLUMN IF NOT EXISTS scenario_id                    STRING,
  ADD COLUMN IF NOT EXISTS s_stack_trace_present          FLOAT64,
  ADD COLUMN IF NOT EXISTS s_line_number_matches_source   FLOAT64,
  ADD COLUMN IF NOT EXISTS s_known_fix_pattern_match      FLOAT64,
  ADD COLUMN IF NOT EXISTS s_log_completeness             FLOAT64,
  ADD COLUMN IF NOT EXISTS s_history_merge_rate           FLOAT64,
  ADD COLUMN IF NOT EXISTS s_retrieval_support            FLOAT64,
  ADD COLUMN IF NOT EXISTS s_external_dependency_detected FLOAT64,
  ADD COLUMN IF NOT EXISTS weights_version                STRING;

-- Sanity check after the migration: every recent row should have a non-null
-- s_line_number_matches_source. If this comes back all-NULL, the worker is
-- still running the pre-migration image.
--
-- SELECT COUNT(*) AS rows_7d,
--        COUNTIF(s_line_number_matches_source IS NOT NULL) AS with_new_signals
-- FROM `dag_failure_agent.confidence_signals`
-- WHERE created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);
