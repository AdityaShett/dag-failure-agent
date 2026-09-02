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
    diff_applied: Optional[bool]   # <-- ADDED: tracks whether a real diff was
                                    #     applied vs. fallback-filler text was
                                    #     used, so PR titles/labels stay honest
                                    #     (see pr.py, PROJECT-HANDOFF.md §6.2/§6.1)
    llm_confidence: Optional[float]
    confidence_score: Optional[float]
    confidence_tier: Optional[str]
    confidence_record_id: Optional[str]
