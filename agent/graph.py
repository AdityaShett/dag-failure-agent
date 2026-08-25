from langgraph.graph import StateGraph, START, END
from agent.state import RCAState
from agent.nodes import (
    collect_context,
    retrieve_knowledge,
    analyze_root_cause,
    generate_fix,
    compute_confidence,
    _mark_confidence_no_pr,
)
from agent.pr import open_draft_pr


def route_after_fix(state: RCAState) -> str:
    if state.get("proposed_fix", "").strip().startswith("NO_CONFIDENT_FIX"):
        return "notify_human_no_fix"
    if state.get("confidence_tier") == "low":
        return "notify_human_no_fix"
    return "open_pr"

def notify_human_no_fix(state: RCAState) -> dict:
    print(
        f"NOTIFY: dag={state.get('dag_id')} task={state.get('task_id')} "
        f"confidence_score={state.get('confidence_score')} "
        f"tier={state.get('confidence_tier')}\n"
        f"root_cause={state.get('root_cause')}"
    )
    _mark_confidence_no_pr(state.get("confidence_record_id"))   # add this line
    return {}

graph = StateGraph(RCAState)
graph.add_node("collect_context", collect_context)
graph.add_node("retrieve_knowledge", retrieve_knowledge)
graph.add_node("analyze_root_cause", analyze_root_cause)
graph.add_node("generate_fix", generate_fix)
graph.add_node("compute_confidence", compute_confidence)
graph.add_node("open_pr", open_draft_pr)
graph.add_node("notify_human_no_fix", notify_human_no_fix)

graph.add_edge(START, "collect_context")
graph.add_edge("collect_context", "retrieve_knowledge")
graph.add_edge("retrieve_knowledge", "analyze_root_cause")
graph.add_edge("analyze_root_cause", "generate_fix")
graph.add_edge("generate_fix", "compute_confidence")
graph.add_conditional_edges("compute_confidence", route_after_fix, {
    "open_pr": "open_pr",
    "notify_human_no_fix": "notify_human_no_fix",
})
graph.add_edge("open_pr", END)
graph.add_edge("notify_human_no_fix", END)

app = graph.compile()