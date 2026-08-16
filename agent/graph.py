from langgraph.graph import StateGraph, START, END
from agent.state import RCAState
from agent.nodes import collect_context , retrieve_knowledge, analyze_root_cause, generate_fix
from agent.pr import open_draft_pr

def route_after_fix(state: RCAState) -> str:
    if state.get("proposed_fix", "").strip() == "NO_CONFIDENT_FIX":
        return "notify_human_no_fix"
    return "open_pr"

def notify_human_no_fix(state : RCAState) -> dict:
    return {}

graph = StateGraph(RCAState)
graph.add_node("collect_context", collect_context)
graph.add_node("retrieve_knowledge", retrieve_knowledge)
graph.add_node("analyze_root_cause", analyze_root_cause)
graph.add_node("generate_fix", generate_fix)
graph.add_node("open_pr", open_draft_pr)
graph.add_node("notify_human_no_fix", notify_human_no_fix)

graph.add_edge(START, "collect_context")
graph.add_edge("collect_context", "retrieve_knowledge")
graph.add_edge("retrieve_knowledge", "analyze_root_cause")
graph.add_edge("analyze_root_cause", "generate_fix")
graph.add_conditional_edges("generate_fix", route_after_fix, {

    "open_pr" : "open_pr",
    "notify_human_no_fix" : "notify_human_no_fix",
})

graph.add_edge("open_pr", END)
graph.add_edge("notify_human_no_fix", END)

app = graph.compile()


