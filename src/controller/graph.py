"""
Main LangGraph state machine for the Idaho Federated AI Swarm controller.

Orchestrates the complete query flow:
1. Planning: Analyze question and route to agencies
2. Execution: IDHW runs first (has family linkage data); parent IDs are extracted;
   then IDJC and IDOC run in parallel using those parent IDs
3. Reasoning: CrossAgencyReasoner builds ephemeral graph and counts matches
4. Answer: Synthesize natural language response

Graph topology (cross-agency with IDHW):
  intent_node
    -> planner_node
    -> router_node
    -> execute_idhw  (sequential — must complete first)
    -> extract_parent_ids_node
    -> [execute_idjc, execute_idoc]  (parallel fan-out)
    -> reasoning_node  (CrossAgencyReasoner)
    -> answer_node

For queries that don't need IDHW the router sends directly to extract_parent_ids
(which is a no-op when idhw_data is empty) and then fans out to the relevant
IDJC / IDOC nodes.
"""

import logging
from typing import Any

from langgraph.graph import StateGraph, START, END
from langgraph.graph.state import CompiledStateGraph
from langgraph.checkpoint.memory import MemorySaver

from shared.schemas import InsightState, AgencyName
from .planner import plan_query
from .executor import execute_idhw, execute_idjc, execute_idoc, extract_parent_ids_node
from .answer import synthesize_answer
from reasoning.cross_agency import reasoning_node as cross_agency_reasoning_node

logger = logging.getLogger(__name__)


async def intent_node(state: InsightState) -> dict:
    """Initial node: seed empty collections so reducers have a base."""
    return {
        "idhw_data": {},
        "idjc_data": {},
        "idoc_data": {},
        "identity_matches": {},
        "reasoning_result": {},
        "parent_ids": [],
        "child_to_parents": {},
        "errors": [],
        "execution_trace": ["Starting query execution"],
        "sources": [],
    }


async def router_node(state: InsightState) -> dict:
    """Route node: log which agencies will be queried."""
    agencies = state.get("agencies", [])
    return {
        "execution_trace": [
            f"Routing to agencies: {[a.value for a in agencies]}"
        ],
    }


def _needs_idhw_first(state: InsightState) -> str:
    """
    Conditional edge: decide whether IDHW must run first.

    Returns 'execute_idhw' when IDHW is in the plan.
    Returns 'skip_idhw' when IDHW is not required.
    """
    agencies = state.get("agencies", [])
    if AgencyName.IDHW in agencies:
        return "execute_idhw"
    return "skip_idhw"


def _route_after_extract(state: InsightState) -> list[str]:
    """
    After parent IDs are extracted, fan out to whichever of IDJC/IDOC were requested.
    If neither was requested (IDHW-only query), go straight to reasoning.
    """
    agencies = state.get("agencies", [])
    next_nodes = []
    if AgencyName.IDJC in agencies:
        next_nodes.append("execute_idjc")
    if AgencyName.IDOC in agencies:
        next_nodes.append("execute_idoc")
    if not next_nodes:
        next_nodes.append("reasoning")
    return next_nodes


def build_graph() -> CompiledStateGraph:
    """
    Build and compile the LangGraph state machine.

    Returns:
        Compiled state graph
    """
    graph = StateGraph(InsightState)

    # ── Nodes ─────────────────────────────────────────────────────────────────
    graph.add_node("intent", intent_node)
    graph.add_node("planner", plan_query)
    graph.add_node("router", router_node)
    graph.add_node("execute_idhw", execute_idhw)
    graph.add_node("extract_parent_ids", extract_parent_ids_node)
    graph.add_node("execute_idjc", execute_idjc)
    graph.add_node("execute_idoc", execute_idoc)
    graph.add_node("reasoning", cross_agency_reasoning_node)
    graph.add_node("answer", synthesize_answer)

    # ── Linear spine ──────────────────────────────────────────────────────────
    graph.add_edge(START, "intent")
    graph.add_edge("intent", "planner")
    graph.add_edge("planner", "router")

    # ── Router: IDHW-first path vs skip-IDHW path ────────────────────────────
    graph.add_conditional_edges(
        "router",
        _needs_idhw_first,
        {
            "execute_idhw": "execute_idhw",
            "skip_idhw": "extract_parent_ids",   # no-op when idhw_data empty
        },
    )

    # IDHW completes → extract parent IDs (sequential dependency)
    graph.add_edge("execute_idhw", "extract_parent_ids")

    # extract_parent_ids → fan-out to IDJC and/or IDOC (or straight to reasoning)
    graph.add_conditional_edges(
        "extract_parent_ids",
        _route_after_extract,
        {
            "execute_idjc": "execute_idjc",
            "execute_idoc": "execute_idoc",
            "reasoning": "reasoning",
        },
    )

    # Parallel agency nodes converge to reasoning
    graph.add_edge("execute_idjc", "reasoning")
    graph.add_edge("execute_idoc", "reasoning")

    # Reasoning → answer → END
    graph.add_edge("reasoning", "answer")
    graph.add_edge("answer", END)

    # ── Compile ───────────────────────────────────────────────────────────────
    memory = MemorySaver()
    return graph.compile(checkpointer=memory)


# Global compiled graph instance
_graph: CompiledStateGraph | None = None


def get_graph() -> CompiledStateGraph:
    """Get or build the compiled graph (singleton)."""
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph


async def run_query(question: str, thread_id: str = "default_thread", progress_callback=None) -> dict[str, Any]:
    """
    Run a query through the complete federated AI swarm.

    Args:
        question: Natural language question
        thread_id: Conversation thread identifier for memory
        progress_callback: Optional callable for live node execution updates

    Returns:
        Dictionary with answer, confidence, sources, intent, errors, execution_trace.
    """
    graph = get_graph()

    initial_state: InsightState = {
        "question": question,
        "intent": None,
        "plan": [],
        "agencies": [],
        "idhw_data": {},
        "idjc_data": {},
        "idoc_data": {},
        "identity_matches": {},
        "reasoning_result": {},
        "answer": "",
        "confidence": 0.0,
        "parent_ids": [],
        "child_to_parents": {},
        "sources": [],
        "errors": [],
        "execution_trace": [],
    }

    try:
        config = {"configurable": {"thread_id": thread_id}}
        
        if progress_callback:
            import inspect
            import asyncio
            try:
                # stream_mode="updates" yields {node_name: {state_update_dict}}
                async for chunk in graph.astream(initial_state, config=config, stream_mode="updates"):
                    for node_name, state_update in chunk.items():
                        if inspect.iscoroutinefunction(progress_callback):
                            await progress_callback(node_name, state_update)
                        else:
                            progress_callback(node_name, state_update)
            except asyncio.CancelledError:
                raise
            except Exception as stream_err:
                logger.error(f"Streaming trace error: {stream_err}")
                
            snapshot = graph.get_state(config)
            final_state = snapshot.values if snapshot else {}
        else:
            final_state = await graph.ainvoke(initial_state, config=config)

        return {
            "answer": final_state.get("answer", ""),
            "confidence": final_state.get("confidence", 0.0),
            "sources": final_state.get("sources", []),
            "intent": final_state.get("intent").value if final_state.get("intent") else None,
            "errors": final_state.get("errors", []),
            "execution_trace": final_state.get("execution_trace", []),
        }
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        return {
            "answer": "An error occurred while processing your query.",
            "confidence": 0.0,
            "sources": [],
            "intent": None,
            "errors": [str(e)],
            "execution_trace": [],
        }
