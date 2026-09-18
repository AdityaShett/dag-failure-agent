

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
