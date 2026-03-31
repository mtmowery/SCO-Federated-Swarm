"""
Main LangGraph state machine for the Idaho Federated AI Swarm controller.

Orchestrates the complete query flow:
1. Planning: Analyze question and route to agencies
2. Execution: Query agency MCP servers in parallel
3. Reasoning: Cross-reference results and resolve identities
4. Answer: Synthesize natural language response

Graph topology:
  intent_node
    -> planner_node
    -> router_node
    -> [idhw_node, idjc_node, idoc_node] (parallel)
    -> reasoning_node
    -> answer_node
"""

import logging
from typing import Any

from langgraph.graph import StateGraph, START, END
from langgraph.graph.state import CompiledStateGraph
from langgraph.checkpoint.memory import MemorySaver

from shared.schemas import InsightState, AgencyName
from shared.config import settings
from .planner import plan_query
from .executor import execute_idhw, execute_idjc, execute_idoc
from .answer import synthesize_answer

logger = logging.getLogger(__name__)


async def intent_node(state: InsightState) -> dict:
    """Initial node: validate and prepare state."""
    return {
        "idhw_data": {},
        "idjc_data": {},
        "idoc_data": {},
        "identity_matches": {},
        "reasoning_result": {},
        # Lists use operator.add reducer, so these seed the initial values
        "errors": [],
        "execution_trace": ["Starting query execution"],
        "sources": [],
    }


async def router_node(state: InsightState) -> dict:
    """Route node: log which agencies will be queried."""
    agencies = state.get("agencies", [])
    return {
        "execution_trace": [
            f"Routing to {len(agencies)} agencies: {[a.value for a in agencies]}"
        ],
    }


async def reasoning_node(state: InsightState) -> dict:
    """Reasoning node: cross-reference and match identities."""
    traces = ["Beginning cross-agency reasoning and identity matching"]

    reasoning_result = {
        "idhw_data": state.get("idhw_data", {}),
        "idjc_data": state.get("idjc_data", {}),
        "idoc_data": state.get("idoc_data", {}),
    }

    identity_matches = {}

    agencies_with_data = [
        agency
        for agency in [AgencyName.IDHW, AgencyName.IDJC, AgencyName.IDOC]
        if state.get(f"{agency.value}_data", {})
    ]

    if len(agencies_with_data) > 1:
        identity_matches = await _match_identities(
            idhw_data=state.get("idhw_data", {}),
            idjc_data=state.get("idjc_data", {}),
            idoc_data=state.get("idoc_data", {}),
        )
        reasoning_result["identity_matches"] = identity_matches

        if identity_matches.get("matches"):
            traces.append(
                f"Cross-agency identity resolution: "
                f"{len(identity_matches['matches'])} identities matched"
            )

    return {
        "reasoning_result": reasoning_result,
        "identity_matches": identity_matches,
        "execution_trace": traces,
    }


async def _match_identities(
    idhw_data: dict[str, Any],
    idjc_data: dict[str, Any],
    idoc_data: dict[str, Any],
) -> dict[str, Any]:
    """
    Perform cross-agency identity matching.

    Matches individuals across IDHW (family), IDJC (youth), and IDOC (adult)
    using insight_id, SSN, and name/DOB combinations.

    Args:
        idhw_data: IDHW query results
        idjc_data: IDJC query results
        idoc_data: IDOC query results

    Returns:
        Identity matches dictionary with structure:
        {
            "matches": [
                {
                    "insight_id": "...",
                    "agencies": ["idhw", "idoc"],
                    "confidence": 0.95
                }
            ]
        }
    """
    matches = []

    # Extract identifiers from IDHW
    idhw_identifiers = _extract_identifiers_idhw(idhw_data)

    # Extract identifiers from IDJC
    idjc_identifiers = _extract_identifiers_idjc(idjc_data)

    # Extract identifiers from IDOC
    idoc_identifiers = _extract_identifiers_idoc(idoc_data)

    # Match across agencies
    # Priority: insight_id > SSN > (name + DOB)

    # IDHW to IDOC matching (parent-child relationship check)
    for idhw_id, idhw_info in idhw_identifiers.items():
        for idoc_id, idoc_info in idoc_identifiers.items():
            # Check if IDOC person matches IDHW parent
            if _is_identity_match(idhw_info, idoc_info):
                matches.append({
                    "insight_id": idoc_id or idhw_id,
                    "agencies": ["idhw", "idoc"],
                    "confidence": _calculate_match_confidence(idhw_info, idoc_info),
                    "idhw_id": idhw_id,
                    "idoc_id": idoc_id,
                })

    # IDHW to IDJC matching (child-youth transition)
    for idhw_id, idhw_info in idhw_identifiers.items():
        for idjc_id, idjc_info in idjc_identifiers.items():
            if _is_identity_match(idhw_info, idjc_info):
                # Check if not already matched
                if not any(
                    m["idhw_id"] == idhw_id and m.get("idjc_id") == idjc_id
                    for m in matches
                ):
                    matches.append({
                        "insight_id": idjc_id or idhw_id,
                        "agencies": ["idhw", "idjc"],
                        "confidence": _calculate_match_confidence(idhw_info, idjc_info),
                        "idhw_id": idhw_id,
                        "idjc_id": idjc_id,
                    })

    # IDJC to IDOC matching (youth-adult transition)
    for idjc_id, idjc_info in idjc_identifiers.items():
        for idoc_id, idoc_info in idoc_identifiers.items():
            if _is_identity_match(idjc_info, idoc_info):
                matches.append({
                    "insight_id": idoc_id or idjc_id,
                    "agencies": ["idjc", "idoc"],
                    "confidence": _calculate_match_confidence(idjc_info, idoc_info),
                    "idjc_id": idjc_id,
                    "idoc_id": idoc_id,
                })

    return {"matches": matches, "total_matches": len(matches)}


def _extract_identifiers_idhw(idhw_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Extract identifiers from IDHW data."""
    identifiers = {}

    for child in idhw_data.get("child_records", []):
        if isinstance(child, dict):
            key = child.get("insight_id") or child.get("child_insight_id")
            if key:
                identifiers[key] = {
                    "insight_id": key,
                    "ssn": child.get("ssn"),
                    "name": f"{child.get('first_name', '')} {child.get('last_name', '')}".strip(),
                    "dob": child.get("dob"),
                    "agency": "idhw",
                }

    for rel in idhw_data.get("family_relationships", []):
        if isinstance(rel, dict):
            # Add parent identifiers
            mother_id = rel.get("mother_insight_id")
            if mother_id:
                identifiers[mother_id] = {
                    "insight_id": mother_id,
                    "ssn": rel.get("mother_ssn"),
                    "name": rel.get("mother_name"),
                    "dob": rel.get("mother_dob"),
                    "agency": "idhw",
                    "relationship": "parent",
                }

            father_id = rel.get("father_insight_id")
            if father_id:
                identifiers[father_id] = {
                    "insight_id": father_id,
                    "ssn": rel.get("father_ssn"),
                    "name": rel.get("father_name"),
                    "dob": rel.get("father_dob"),
                    "agency": "idhw",
                    "relationship": "parent",
                }

    return identifiers


def _extract_identifiers_idjc(idjc_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Extract identifiers from IDJC data."""
    identifiers = {}

    for commitment in idjc_data.get("commitments", []):
        if isinstance(commitment, dict):
            key = commitment.get("insight_id") or commitment.get("ijos_id")
            if key:
                identifiers[key] = {
                    "insight_id": key,
                    "ssn": commitment.get("ssn"),
                    "name": f"{commitment.get('first_name', '')} {commitment.get('last_name', '')}".strip(),
                    "dob": commitment.get("dob"),
                    "agency": "idjc",
                }

    return identifiers


def _extract_identifiers_idoc(idoc_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Extract identifiers from IDOC data."""
    identifiers = {}

    for inmate in idoc_data.get("inmates", []):
        if isinstance(inmate, dict):
            key = inmate.get("insight_id") or inmate.get("ofndr_num")
            if key:
                identifiers[key] = {
                    "insight_id": key,
                    "ssn": inmate.get("ssn_nbr"),
                    "name": f"{inmate.get('fnam', '')} {inmate.get('lnam', '')}".strip(),
                    "dob": inmate.get("dob_dtd"),
                    "agency": "idoc",
                }

    return identifiers


def _is_identity_match(info1: dict[str, Any], info2: dict[str, Any]) -> bool:
    """
    Determine if two identifiers match.

    Uses priority: insight_id > SSN > (name + DOB)

    Args:
        info1: First identifier dict
        info2: Second identifier dict

    Returns:
        True if identities match
    """
    # Check insight_id match
    id1 = info1.get("insight_id")
    id2 = info2.get("insight_id")
    if id1 and id2 and id1 == id2:
        return True

    # Check SSN match
    ssn1 = info1.get("ssn")
    ssn2 = info2.get("ssn")
    if ssn1 and ssn2 and ssn1 == ssn2:
        return True

    # Check name + DOB match
    name1 = (info1.get("name") or "").lower().strip()
    name2 = (info2.get("name") or "").lower().strip()
    dob1 = info1.get("dob")
    dob2 = info2.get("dob")

    if name1 and name2 and dob1 and dob2:
        if name1 == name2 and dob1 == dob2:
            return True

    return False


def _calculate_match_confidence(info1: dict[str, Any], info2: dict[str, Any]) -> float:
    """
    Calculate confidence score for identity match.

    Args:
        info1: First identifier dict
        info2: Second identifier dict

    Returns:
        Confidence score (0.0 to 1.0)
    """
    # Perfect match: insight_id
    if info1.get("insight_id") and info2.get("insight_id"):
        if info1.get("insight_id") == info2.get("insight_id"):
            return 1.0

    # Very high confidence: SSN
    if info1.get("ssn") and info2.get("ssn"):
        if info1.get("ssn") == info2.get("ssn"):
            return 0.99

    # High confidence: name + DOB
    if info1.get("name") and info2.get("name") and info1.get("dob") and info2.get("dob"):
        if (
            (info1.get("name") or "").lower() == (info2.get("name") or "").lower()
            and info1.get("dob") == info2.get("dob")
        ):
            return 0.95

    return 0.5


def _route_to_agency_nodes(state: InsightState) -> list[str]:
    """
    Determine which agency nodes to execute.

    Args:
        state: InsightState with agencies

    Returns:
        List of next node names
    """
    agencies = state.get("agencies", [])
    next_nodes = []

    if AgencyName.IDHW in agencies:
        next_nodes.append("execute_idhw")
    if AgencyName.IDJC in agencies:
        next_nodes.append("execute_idjc")
    if AgencyName.IDOC in agencies:
        next_nodes.append("execute_idoc")

    if not next_nodes:
        next_nodes = ["execute_idhw", "execute_idjc", "execute_idoc"]

    return next_nodes


def build_graph() -> CompiledStateGraph:
    """
    Build and compile the LangGraph state machine.

    Topology:
        intent_node
        -> planner_node
        -> router_node
        -> [execute_idhw, execute_idjc, execute_idoc]
        -> reasoning_node
        -> answer_node
        -> END

    Returns:
        Compiled state graph
    """
    graph = StateGraph(InsightState)

    # Add nodes
    graph.add_node("intent", intent_node)
    graph.add_node("planner", plan_query)
    graph.add_node("router", router_node)
    graph.add_node("execute_idhw", execute_idhw)
    graph.add_node("execute_idjc", execute_idjc)
    graph.add_node("execute_idoc", execute_idoc)
    graph.add_node("reasoning", reasoning_node)
    graph.add_node("answer", synthesize_answer)

    # Add edges
    graph.add_edge("intent", "planner")
    graph.add_edge("planner", "router")

    # Router to agency nodes - use conditional routing
    graph.add_conditional_edges(
        "router",
        lambda state: _route_to_agency_nodes(state),
        {
            "execute_idhw": "execute_idhw",
            "execute_idjc": "execute_idjc",
            "execute_idoc": "execute_idoc",
        },
    )

    # All agency nodes converge to reasoning
    graph.add_edge("execute_idhw", "reasoning")
    graph.add_edge("execute_idjc", "reasoning")
    graph.add_edge("execute_idoc", "reasoning")

    # Reasoning to answer
    graph.add_edge("reasoning", "answer")

    # Answer to end
    graph.add_edge("answer", END)

    # Set entry point
    graph.add_edge(START, "intent")

    # Add memory checkpointer
    memory = MemorySaver()

    # Compile
    return graph.compile(checkpointer=memory)


# Global compiled graph instance
_graph: CompiledStateGraph | None = None



def get_graph() -> CompiledStateGraph:
    """Get or build the compiled graph."""
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph


async def run_query(question: str, thread_id: str = "default_thread") -> dict[str, Any]:
    """
    Run a query through the complete federated AI swarm.

    Args:
        question: Natural language question
        thread_id: Conversation thread identifier for memory


    Returns:
        Dictionary with:
        - answer: str - Natural language answer
        - confidence: float - Confidence score
        - sources: list[str] - Contributing agencies
        - intent: str - Query intent classification
        - errors: list[str] - Any errors encountered
        - execution_trace: list[str] - Execution log
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
        "sources": [],
        "errors": [],
        "execution_trace": [],
    }

    try:
        config = {"configurable": {"thread_id": thread_id}}
        final_state = await graph.ainvoke(initial_state, config=config)

        return {
            "answer": final_state.get("answer", ""),
            "confidence": final_state.get("confidence", 0.0),
            "sources": final_state.get("sources", []),
            "intent": final_state.get("intent", "").value if final_state.get("intent") else None,
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
