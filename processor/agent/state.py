
from typing import TypedDict, Optional, List

class RCAState(TypedDict, total=False):
    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    task_logs: str
    dag_source: str
    github_repo: str
    target_file: str
    synthetic_task_logs: Optional[str]
    retrieved_knowledge: List[str]
    root_cause: Optional[str]
    proposed_fix: Optional[str]
    confidence: str
    pr_url: Optional[str]
    diff_applied: Optional[bool]
    confidence_score: Optional[float]
    confidence_tier: Optional[str]
    confidence_record_id: Optional[str]
    confidence_signals: Optional[dict]
    confidence_contributions: Optional[dict]
    confidence_threshold: Optional[float]
    meets_confidence_threshold: Optional[bool]
    gate_reason: Optional[str]
    source_ref: Optional[str]
    scenario_id: Optional[str]
    branch_name: Optional[str]
    failure_type: Optional[str]
    difficulty: Optional[str]
    expected_outcome: Optional[str]

