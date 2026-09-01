SELECT s.confidence_tier, o.outcome, COUNT(*) AS n
FROM `dag-failure-agent-505623.dag_failure_agent.confidence_signals` s
JOIN (
  SELECT record_id, outcome,
         ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY updated_at DESC) AS rn
  FROM `dag-failure-agent-505623.dag_failure_agent.confidence_outcomes`
) o ON o.record_id = s.record_id AND o.rn = 1
GROUP BY s.confidence_tier, o.outcome
ORDER BY s.confidence_tier
