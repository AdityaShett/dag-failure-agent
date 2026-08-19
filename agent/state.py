from typing import TypedDict, Optional, List

class RCAState(TypedDict, total = False):
    dag_id : str
    task_id :str
    run_id : str
    try_number : int
    task_logs : str
    dag_source : str
    github_repo: str
    target_file: str
    retrieved_knowledge: List[str]
    root_cause : Optional[str]
    proposed_fix : Optional[str]
    confidence : str
    pr_url : Optional[str]
    llm_confidence: Optional[float]
    confidence_score: Optional[float]
    confidence_tier: Optional[str]