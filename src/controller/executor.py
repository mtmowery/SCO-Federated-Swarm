"""
Executor agent nodes for querying agency MCP servers.

Implements separate executor nodes for each agency:
- execute_idhw: Foster care and family relationships
- execute_idjc: Juvenile justice records
- execute_idoc: Adult incarceration records

Each executor checks if their agency is in the plan, calls appropriate MCP tools,
and updates state with results and traces.
"""

import logging
from typing import Any

from shared.schemas import InsightState, AgencyName
from shared.config import settings
from .mcp_client import MCPClient

logger = logging.getLogger(__name__)


async def execute_idhw(state: InsightState) -> dict:
    """
    Execute IDHW (foster care) queries.

    Returns only the keys this node modifies — critical for
    parallel execution in LangGraph (avoids concurrent key writes).
    """
    agencies = state.get("agencies", [])
    traces = []
    errors = []
    sources = []
    idhw_data = {}

    if AgencyName.IDHW not in agencies:
        return {"idhw_data": {}, "sources": [], "errors": [], "execution_trace": []}

    traces.append("Executing IDHW queries...")

    try:
        async with MCPClient(
            endpoints=settings.mcp.endpoints,
            timeout=settings.mcp.timeout,
        ) as client:
            question = state.get("question", "")
            plan = state.get("plan", [])
            query_params = _extract_query_params_idhw(question)

            family_data = {}
            if any("family" in step.lower() or "relationship" in step.lower() for step in plan):
                try:
                    family_data = await client.execute_tool(
                        "idhw", "get_family_relationships", {}
                    )
                    traces.append(
                        f"IDHW family relationships: {len(family_data.get('relationships', []))} found"
                    )
                except Exception as e:
                    errors.append(f"IDHW family lookup failed: {e}")
                    logger.error(f"IDHW family lookup failed: {e}")

            child_data = {}
            if any("child" in step.lower() for step in plan):
                try:
                    child_data = await client.execute_tool(
                        "idhw", "get_children", {}
                    )
                    traces.append(
                        f"IDHW child records: {len(child_data.get('children', []))} found"
                    )
                except Exception as e:
                    errors.append(f"IDHW child lookup failed: {e}")
                    logger.error(f"IDHW child lookup failed: {e}")

            idhw_data = {
                "family_relationships": family_data.get("relationships", []),
                "child_records": child_data.get("children", []),
            }
            sources.append("idhw")

            parent_ids = _extract_parent_ids(family_data)
            if parent_ids:
                traces.append(f"Extracted {len(parent_ids)} parent IDs for cross-agency matching")

    except Exception as e:
        errors.append(f"IDHW execution failed: {e}")
        logger.error(f"IDHW execution failed: {e}")

    return {
        "idhw_data": idhw_data,
        "sources": sources,
        "errors": errors,
        "execution_trace": traces,
    }


async def execute_idjc(state: InsightState) -> dict:
    """
    Execute IDJC (juvenile justice) queries.

    Returns only the keys this node modifies.
    """
    agencies = state.get("agencies", [])
    traces = []
    errors = []
    sources = []
    idjc_data = {}

    if AgencyName.IDJC not in agencies:
        return {"idjc_data": {}, "sources": [], "errors": [], "execution_trace": []}

    traces.append("Executing IDJC queries...")

    try:
        async with MCPClient(
            endpoints=settings.mcp.endpoints,
            timeout=settings.mcp.timeout,
        ) as client:
            question = state.get("question", "")
            plan = state.get("plan", [])
            query_params = _extract_query_params_idjc(question)

            if AgencyName.IDHW in agencies:
                child_ids = _extract_child_ids(state.get("idhw_data", {}))
                if child_ids:
                    query_params["insight_ids"] = child_ids

            commitment_data = {}
            if any(
                kw in " ".join(plan).lower() + " " + question.lower()
                for kw in ["detention", "commitment", "juvenile", "youth", "idjc", "kids", "children", "offender"]
            ):
                try:
                    if "insight_ids" in query_params and query_params["insight_ids"]:
                        commitment_data = await client.execute_tool(
                            "idjc", "get_people_bulk", {"insight_ids": query_params["insight_ids"]}
                        )
                        if isinstance(commitment_data, list):
                            idjc_data = {"commitments": commitment_data}
                            count = len(commitment_data)
                        else:
                            idjc_data = {"commitments": commitment_data.get("results", {})}
                            count = commitment_data.get('count', 0)
                        traces.append(
                            f"IDJC cross-agency match: {count} people matched"
                        )
                    else:
                        commitment_data = await client.execute_tool(
                            "idjc", "get_commitments", {"limit": 1000}
                        )
                        if isinstance(commitment_data, list):
                            idjc_data = {"commitments": commitment_data}
                            count = len(commitment_data)
                        else:
                            idjc_data = {"commitments": commitment_data.get("commitments", [])}
                            count = commitment_data.get('count', 0)
                        traces.append(
                            f"IDJC commitments: {count} found"
                        )
                except Exception as e:
                    errors.append(f"IDJC commitment lookup failed: {e}")
                    logger.error(f"IDJC commitment lookup failed: {e}")
                    idjc_data = {"commitments": []}

            else:
                idjc_data = {"commitments": []}
            sources.append("idjc")

    except Exception as e:
        errors.append(f"IDJC execution failed: {e}")
        logger.error(f"IDJC execution failed: {e}")

    return {
        "idjc_data": idjc_data,
        "sources": sources,
        "errors": errors,
        "execution_trace": traces,
    }


async def execute_idoc(state: InsightState) -> dict:
    """
    Execute IDOC (adult incarceration) queries.

    Returns only the keys this node modifies.
    """
    agencies = state.get("agencies", [])
    traces = []
    errors = []
    sources = []
    idoc_data = {}

    if AgencyName.IDOC not in agencies:
        return {"idoc_data": {}, "sources": [], "errors": [], "execution_trace": []}

    traces.append("Executing IDOC queries...")

    try:
        async with MCPClient(
            endpoints=settings.mcp.endpoints,
            timeout=settings.mcp.timeout,
        ) as client:
            question = state.get("question", "")
            plan = state.get("plan", [])
            query_params = _extract_query_params_idoc(question)

            if AgencyName.IDHW in agencies:
                parent_ids = _extract_parent_ids(state.get("idhw_data", {}))
                if parent_ids:
                    query_params["insight_ids"] = parent_ids

            inmate_data = {}
            if any(
                kw in " ".join(plan).lower()
                for kw in ["inmate", "incarcerat", "prison", "offender"]
            ):
                try:
                    if "insight_ids" in query_params and query_params["insight_ids"]:
                        # We have specific parent IDs to check
                        inmate_data = await client.execute_tool(
                            "idoc", "get_people_bulk", {"insight_ids": query_params["insight_ids"]}
                        )
                        if isinstance(inmate_data, list):
                            idoc_data = {"inmates": inmate_data}
                            count = len(inmate_data)
                        else:
                            idoc_data = {"inmates": inmate_data.get("results", {})}
                            count = inmate_data.get('count', 0)
                        traces.append(
                            f"IDOC cross-agency match: {count} people matched"
                        )
                    else:
                        inmate_data = await client.execute_tool(
                            "idoc", "get_active_offenders", {"limit": 1000}
                        )
                        if isinstance(inmate_data, list):
                            idoc_data = {"inmates": inmate_data}
                            count = len(inmate_data)
                        else:
                            idoc_data = {"inmates": inmate_data.get("offenders", [])}
                            count = inmate_data.get('count', 0)
                        traces.append(
                            f"IDOC active offenders: {count} found"
                        )
                except Exception as e:
                    errors.append(f"IDOC inmate lookup failed: {e}")
                    logger.error(f"IDOC inmate lookup failed: {e}")
                    idoc_data = {"inmates": []}
            else:
                idoc_data = {"inmates": []}
            sources.append("idoc")

    except Exception as e:
        errors.append(f"IDOC execution failed: {e}")
        logger.error(f"IDOC execution failed: {e}")

    return {
        "idoc_data": idoc_data,
        "sources": sources,
        "errors": errors,
        "execution_trace": traces,
    }


def _extract_query_params_idhw(question: str) -> dict[str, Any]:
    """
    Extract IDHW-specific query parameters from question.

    Args:
        question: Natural language question

    Returns:
        Query parameters dict
    """
    params: dict[str, Any] = {}

    # Simple extraction - in production, use NER or more sophisticated parsing
    question_lower = question.lower()

    if "name" in question_lower or "named" in question_lower:
        params["include_names"] = True

    if "address" in question_lower:
        params["include_addresses"] = True

    return params


def _extract_query_params_idjc(question: str) -> dict[str, Any]:
    """
    Extract IDJC-specific query parameters from question.

    Args:
        question: Natural language question

    Returns:
        Query parameters dict
    """
    params: dict[str, Any] = {}

    question_lower = question.lower()

    if "current" in question_lower or "active" in question_lower:
        params["active_only"] = True

    if "offense" in question_lower or "charge" in question_lower:
        params["include_offenses"] = True

    return params


def _extract_query_params_idoc(question: str) -> dict[str, Any]:
    """
    Extract IDOC-specific query parameters from question.

    Args:
        question: Natural language question

    Returns:
        Query parameters dict
    """
    params: dict[str, Any] = {}

    question_lower = question.lower()

    if "current" in question_lower or "active" in question_lower:
        params["active_only"] = True

    if "sentence" in question_lower:
        params["include_sentencing"] = True

    if "facility" in question_lower or "location" in question_lower:
        params["include_facility"] = True

    return params


def _extract_parent_ids(idhw_data: dict[str, Any]) -> list[str]:
    """
    Extract parent insight_ids from IDHW family relationship data.

    Used for cross-agency identity matching.

    Args:
        idhw_data: IDHW query results

    Returns:
        List of parent insight_ids
    """
    parent_ids = []

    relationships = idhw_data.get("relationships", [])
    for rel in relationships:
        if isinstance(rel, dict):
            mother_id = rel.get("mother_insight_id")
            father_id = rel.get("father_insight_id")
            if mother_id:
                parent_ids.append(mother_id)
            if father_id:
                parent_ids.append(father_id)

    # Also check family_relationships key
    family_rels = idhw_data.get("family_relationships", [])
    for rel in family_rels:
        if isinstance(rel, dict):
            mother_id = rel.get("mother_insight_id")
            father_id = rel.get("father_insight_id")
            if mother_id:
                parent_ids.append(mother_id)
            if father_id:
                parent_ids.append(father_id)

    # Deduplicate
    return list(set(parent_ids))


def _extract_child_ids(idhw_data: dict[str, Any]) -> list[str]:
    """
    Extract child insight_ids from IDHW data.

    Used for cross-agency identity matching.

    Args:
        idhw_data: IDHW query results

    Returns:
        List of child insight_ids
    """
    child_ids = []

    for child in idhw_data.get("child_records", []):
        if isinstance(child, dict):
            key = child.get("insight_id") or child.get("child_insight_id")
            if key:
                child_ids.append(key)

    # Deduplicate
    return list(set(child_ids))
