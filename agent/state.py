from typing import TypedDict, Optional, List

class RCAState(TypedDict, total = False):
    dag_id : str
    task_id :str
    run_id : str
    try_number : int
    task_logs : str
    dag_source : str
    retrieved_knowledge: List[str]
    root_casue : Optional[str]
    proposed_fix : Optional[str]
    confidence : str
    pr_url : Optional[str]