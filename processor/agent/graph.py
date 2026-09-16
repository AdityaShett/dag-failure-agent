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


def route_after_confidence(state: RCAState) -> str:
    """Two gates, both of which must pass before a PR is opened.

    1. The model has to have produced a diff at all.
    2. The confidence score has to clear config/weights.json's
       confidence_threshold.

    Gate 2 is new. Before, routing looked only at the text of proposed_fix,
    so confidence_threshold was a number that appeared in a config file, got
    logged, got printed in PR bodies -- and never actually decided anything.
    Tuning the weights could not change which PRs got opened, which is why
    weight tuning felt like it "did nothing".
    """
    fix = (state.get("proposed_fix") or "").strip()
    if fix.startswith("NO_CONFIDENT_FIX") or not fix:
        return "notify_human_no_fix"

    if not state.get("meets_confidence_threshold", True):
        return "notify_human_no_fix"

    return "open_pr"


def _gate_reason(state: RCAState) -> str:
    fix = (state.get("proposed_fix") or "").strip()
    if fix.startswith("NO_CONFIDENT_FIX") or not fix:
        return "model returned NO_CONFIDENT_FIX"
    return (f"confidence {state.get('confidence_score')} below threshold "
            f"{state.get('confidence_threshold')}")


def notify_human_no_fix(state: RCAState) -> dict:
    reason = _gate_reason(state)
    print(
        f"NOTIFY: dag={state.get('dag_id')} task={state.get('task_id')} "
        f"reason={reason} "
        f"confidence_score={state.get('confidence_score')} "
        f"tier={state.get('confidence_tier')} "
        f"threshold={state.get('confidence_threshold')}\n"
        f"signals={state.get('confidence_signals')}\n"
        f"root_cause={state.get('root_cause')}"
    )
    _mark_confidence_no_pr(state.get("confidence_record_id"), reason)
    return {"gate_reason": reason}


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
graph.add_conditional_edges("compute_confidence", route_after_confidence, {
    "open_pr": "open_pr",
    "notify_human_no_fix": "notify_human_no_fix",
})
graph.add_edge("open_pr", END)
graph.add_edge("notify_human_no_fix", END)

app = graph.compile()
