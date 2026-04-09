"""
Executor agent nodes for querying agency MCP servers.

Execution order enforced by graph.py topology:
  1. execute_idhw  (sequential — provides family relationships)
  2. extract_parent_ids_node  (sequential — extracts parent insight_ids from idhw_data)
  3. execute_idjc + execute_idoc  (parallel fan-out — both read parent_ids from state)

Data key contract with CrossAgencyReasoner (reasoning/cross_agency.py):
  - idhw_data: {"family_relationships": [...], "child_records": [...]}
  - idjc_data: {"juvenile_ids": set_or_list_of_insight_ids, "commitments": [...]}
  - idoc_data: {"incarcerated_ids": set_or_list_of_insight_ids, "inmates": [...]}
"""

import logging
from typing import Any

from shared.schemas import InsightState, AgencyName, QueryIntent
from shared.config import settings
from .mcp_client import MCPClient

logger = logging.getLogger(__name__)

# Chunk size for SQL IN clause — avoids DB query plan degradation on huge lists
_CHUNK_SIZE = 500


def _chunked(lst: list, size: int):
    """Yield successive chunks of `size` from `lst`."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


# ─────────────────────────────────────────────────────────────────────────────
# extract_parent_ids_node  (sequential step between IDHW and IDJC/IDOC)
# ─────────────────────────────────────────────────────────────────────────────

async def extract_parent_ids_node(state: InsightState) -> dict:
    """
    Extract parent insight_ids from IDHW family relationship data and write
    them to state so IDJC and IDOC executors can use them.

    Also builds child_to_parents mapping for CrossAgencyReasoner.

    This node is a no-op when idhw_data is empty (e.g. non-IDHW queries).
    """
    idhw_data = state.get("idhw_data", {})
    if not idhw_data:
        return {
            "parent_ids": [],
            "child_to_parents": {},
            "execution_trace": [],
        }

    parent_id_set: set[str] = set()
    child_to_parents: dict[str, list] = {}

    # Prefer family_relationships (richer structure)
    relationships = idhw_data.get("family_relationships") or idhw_data.get("relationships", [])
    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        child_id = rel.get("child_insight_id")
        mother_id = rel.get("mother_insight_id")
        father_id = rel.get("father_insight_id")

        parents_for_child = []
        if mother_id:
            parent_id_set.add(mother_id)
            parents_for_child.append({"insight_id": mother_id, "role": "mother"})
        if father_id:
            parent_id_set.add(father_id)
            parents_for_child.append({"insight_id": father_id, "role": "father"})

        if child_id and parents_for_child:
            child_to_parents[child_id] = parents_for_child

    parent_ids = list(parent_id_set)
    trace = f"Extracted {len(parent_ids)} unique parent IDs from {len(relationships)} family relationships"
    logger.info(trace)

    return {
        "parent_ids": parent_ids,
        "child_to_parents": child_to_parents,
        "execution_trace": [trace],
    }


# ─────────────────────────────────────────────────────────────────────────────
# execute_idhw
# ─────────────────────────────────────────────────────────────────────────────

async def execute_idhw(state: InsightState) -> dict:
    """
    Execute IDHW (foster care / family relationships) queries.

    Fetches:
      - family_relationships: child→parent mappings with insight_ids
      - child_records: foster children with care metadata

    Returns only the keys this node modifies.
    """
    agencies = state.get("agencies", [])
    traces: list[str] = []
    errors: list[str] = []
    sources: list[str] = []
    idhw_data: dict[str, Any] = {}

    if AgencyName.IDHW not in agencies:
        return {"idhw_data": {}, "sources": [], "errors": [], "execution_trace": []}

    traces.append("Executing IDHW queries...")

    try:
        async with MCPClient(
            endpoints=settings.mcp.endpoints,
            timeout=settings.mcp.timeout,
        ) as client:
            plan_text = " ".join(state.get("plan", [])).lower()
            question_lower = state.get("question", "").lower()

            family_data: dict = {}
            child_data: dict = {}
            stats_data: dict = {}

            intent = state.get("intent")
            intent_raw = getattr(intent, "value", intent)
            intent_val = str(intent_raw).split(".")[-1].lower()

            # For statistics/single_agency/lookup queries, call get_stats for accurate counts
            if intent_val in ("statistics", "single_agency", "lookup"):
                try:
                    stats_data = await client.execute_tool(
                        "idhw", "get_stats", {}
                    )
                    traces.append(f"IDHW statistics: {stats_data}")
                except Exception as e:
                    errors.append(f"IDHW stats lookup failed: {e}")
                    logger.error(f"IDHW stats lookup failed: {e}")

            # Fetch family relationships for cross-agency or relationship queries
            if intent_val in ("cross_agency", "relationship"):
                try:
                    family_data = await client.execute_tool(
                        "idhw", "get_family_relationships", {}
                    )
                    rel_count = len(family_data.get("relationships", []))
                    traces.append(f"IDHW family relationships: {rel_count} found")
                except Exception as e:
                    errors.append(f"IDHW family lookup failed: {e}")
                    logger.error(f"IDHW family lookup failed: {e}")

            # Fetch foster children records when question involves children, kids, etc or if it's stats
            if "count" in question_lower or any(kw in question_lower + plan_text for kw in ["child", "kid", "foster", "welfare", "youth", "minor"]):
                try:
                    child_data = await client.execute_tool(
                        "idhw", "get_children", {}
                    )
                    child_count = len(child_data.get("children", []))
                    traces.append(f"IDHW child records: {child_count} found")
                except Exception as e:
                    errors.append(f"IDHW child lookup failed: {e}")
                    logger.error(f"IDHW child lookup failed: {e}")

            idhw_data = {
                # Use 'family_relationships' key — matches CrossAgencyReasoner.build_family_graph()
                "family_relationships": family_data.get("relationships", []),
                "child_records": child_data.get("children", []),
                "statistics": stats_data.get("statistics", stats_data),
            }
            sources.append("idhw")

    except Exception as e:
        errors.append(f"IDHW execution failed: {e}")
        logger.error(f"IDHW execution failed: {e}")

    return {
        "idhw_data": idhw_data,
        "sources": sources,
        "errors": errors,
        "execution_trace": traces,
    }


# ─────────────────────────────────────────────────────────────────────────────
# execute_idjc
# ─────────────────────────────────────────────────────────────────────────────

async def execute_idjc(state: InsightState) -> dict:
    """
    Execute IDJC (juvenile justice) queries.

    Uses parent_ids from state (written by extract_parent_ids_node) to look up
    whether any foster-child parents have juvenile records.

    Returns:
      idjc_data = {
          "juvenile_ids": list[str],   # insight_ids with a juvenile record
          "commitments": list[dict],   # full commitment records
      }
    """
    agencies = state.get("agencies", [])
    traces: list[str] = []
    errors: list[str] = []
    sources: list[str] = []
    idjc_data: dict[str, Any] = {}

    if AgencyName.IDJC not in agencies:
        return {"idjc_data": {}, "sources": [], "errors": [], "execution_trace": []}

    traces.append("Executing IDJC queries...")

    try:
        async with MCPClient(
            endpoints=settings.mcp.endpoints,
            timeout=settings.mcp.timeout,
        ) as client:
            # Read parent_ids written by extract_parent_ids_node (guaranteed available)
            parent_ids: list[str] = state.get("parent_ids", [])
            all_commitments: list[dict] = []
            juvenile_ids: set[str] = set()
            statistics: dict = {}

            intent = state.get("intent")
            intent_raw = getattr(intent, "value", intent)
            intent_val = str(intent_raw).split(".")[-1].lower()

            if parent_ids and intent_val not in ("statistics", "single_agency", "lookup"):
                # Cross-agency path: check which parents have juvenile records
                for chunk in _chunked(parent_ids, _CHUNK_SIZE):
                    try:
                        result = await client.execute_tool(
                            "idjc", "check_juvenile_record", {"insight_ids": chunk}
                        )
                        status_map = result.get("results", {}) if isinstance(result, dict) else {}
                        for iid, has_record in status_map.items():
                            if has_record:
                                juvenile_ids.add(iid)
                    except Exception as e:
                        errors.append(f"IDJC bulk lookup chunk failed: {e}")
                        logger.error(f"IDJC bulk lookup chunk failed: {e}")

                traces.append(
                    f"IDJC: {len(juvenile_ids)} parents have juvenile records "
                    f"(checked {len(parent_ids)} parent IDs)"
                )
            elif intent_val in ("statistics", "single_agency", "lookup"):
                # Statistics path: get accurate counts, not a data dump
                try:
                    people_result = await client.execute_tool(
                        "idjc", "count_total_people", {}
                    )
                    status_result = await client.execute_tool(
                        "idjc", "count_by_status", {}
                    )
                    statistics = {
                        "total_people": people_result.get("total_people_count", 0),
                        "by_status": status_result.get("counts", status_result),
                        "total_records": status_result.get("total_records", 0),
                    }
                    traces.append(
                        f"IDJC statistics: {statistics['total_people']} unique people, "
                        f"{statistics['total_records']} total commitment records"
                    )
                except Exception as e:
                    errors.append(f"IDJC statistics lookup failed: {e}")
                    logger.error(f"IDJC statistics lookup failed: {e}")

            # Dynamic keyword extraction for offense matches (runs for any intent)
            question_lower = state.get("question", "").lower()
            keyword = None
            common_keywords = ["murder", "theft", "assault", "robbery", "burglary", "drug", "weapon"]
            for kw in common_keywords:
                if kw in question_lower:
                    keyword = kw
                    break
                    
            if keyword:
                try:
                    offense_result = await client.execute_tool(
                        "idjc", "get_offense_breakdown", {"keyword": keyword}
                    )
                    statistics["offense_breakdown"] = offense_result
                    traces.append(f"IDJC: Found {offense_result['total_people']} people with crimes matching '{keyword}'")
                except Exception as e:
                    errors.append(f"IDJC offense breakdown failed: {e}")

            if "top" in question_lower or "most" in question_lower:
                try:
                    top_result = await client.execute_tool(
                        "idjc", "get_top_offenders", {"limit": 10}
                    )
                    statistics["top_offenders"] = top_result.get("top_offenders", [])
                    traces.append(f"IDJC: Found top {len(statistics['top_offenders'])} offenders")
                except Exception as e:
                    errors.append(f"IDJC top offenders failed: {e}")

            if intent_val == "cross_agency":
                # Cross-agency root: pull ALL insight IDs to build ephemeral graph
                try:
                    result = await client.execute_tool("idjc", "get_all_insight_ids", {})
                    ids = result if isinstance(result, list) else result.get("insight_ids", [])
                    juvenile_ids.update(ids)
                    traces.append(f"IDJC bulk distinct pull: {len(ids)} unique insight_ids found")
                except Exception as e:
                    errors.append(f"IDJC get_all_insight_ids lookup failed: {e}")
                    logger.error(f"IDJC get_all_insight_ids lookup failed: {e}")
            
            elif intent_val not in ("statistics", "single_agency", "lookup"):
                # General fallback: pull commitment list
                try:
                    result = await client.execute_tool(
                        "idjc", "get_commitments", {"limit": 1000}
                    )
                    all_commitments = result if isinstance(result, list) else result.get("commitments", [])
                    for r in all_commitments:
                        if isinstance(r, dict) and r.get("insight_id"):
                            juvenile_ids.add(r["insight_id"])
                    traces.append(f"IDJC commitments (general): {len(all_commitments)} found")
                except Exception as e:
                    errors.append(f"IDJC general lookup failed: {e}")
                    logger.error(f"IDJC general lookup failed: {e}")

            idjc_data = {
                "juvenile_ids": list(juvenile_ids),   # key expected by CrossAgencyReasoner
                "commitments": all_commitments,
                "statistics": statistics,
            }
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


# ─────────────────────────────────────────────────────────────────────────────
# execute_idoc
# ─────────────────────────────────────────────────────────────────────────────

async def execute_idoc(state: InsightState) -> dict:
    """
    Execute IDOC (adult incarceration) queries.

    Uses parent_ids from state (written by extract_parent_ids_node) to look up
    whether any foster-child parents are or were incarcerated.

    Returns:
      idoc_data = {
          "incarcerated_ids": list[str],   # insight_ids with active incarceration
          "inmates": list[dict],           # full inmate records
      }
    """
    agencies = state.get("agencies", [])
    traces: list[str] = []
    errors: list[str] = []
    sources: list[str] = []
    idoc_data: dict[str, Any] = {}

    if AgencyName.IDOC not in agencies:
        return {"idoc_data": {}, "sources": [], "errors": [], "execution_trace": []}

    traces.append("Executing IDOC queries...")

    try:
        async with MCPClient(
            endpoints=settings.mcp.endpoints,
            timeout=settings.mcp.timeout,
        ) as client:
            parent_ids: list[str] = state.get("parent_ids", [])
            all_inmates: list[dict] = []
            incarcerated_ids: set[str] = set()
            statistics: dict = {}

            intent = state.get("intent")
            intent_raw = getattr(intent, "value", intent)
            intent_val = str(intent_raw).split(".")[-1].lower()

            if parent_ids and intent_val not in ("statistics", "single_agency"):
                # Cross-agency path: check which parents are incarcerated
                for chunk in _chunked(parent_ids, _CHUNK_SIZE):
                    try:
                        result = await client.execute_tool(
                            "idoc", "check_incarceration", {"insight_ids": chunk}
                        )
                        status_map = result.get("status", {}) if isinstance(result, dict) else {}
                        for iid, is_active in status_map.items():
                            if is_active:
                                incarcerated_ids.add(iid)
                    except Exception as e:
                        errors.append(f"IDOC bulk lookup chunk failed: {e}")
                        logger.error(f"IDOC bulk lookup chunk failed: {e}")

                traces.append(
                    f"IDOC: {len(incarcerated_ids)} parents have incarceration records "
                    f"(checked {len(parent_ids)} parent IDs)"
                )
            elif intent_val in ("statistics", "single_agency", "lookup"):
                # Statistics path: get accurate counts
                try:
                    people_result = await client.execute_tool(
                        "idoc", "count_total_people", {}
                    )
                    status_result = await client.execute_tool(
                        "idoc", "count_by_status", {}
                    )
                    statistics = {
                        "total_people": people_result.get("total_people_count", 0),
                        "by_status": status_result.get("by_status", status_result),
                        "total_records": status_result.get("total_sentences", 0),
                    }
                    traces.append(
                        f"IDOC statistics: {statistics['total_people']} unique people, "
                        f"{statistics['total_records']} total sentence records"
                    )

                except Exception as e:
                    errors.append(f"IDOC statistics lookup failed: {e}")
                    logger.error(f"IDOC statistics lookup failed: {e}")
            
            # Dynamic keyword extraction for offense matches (runs for any intent)
            question_lower = state.get("question", "").lower()
            keyword = None
            common_keywords = ["murder", "theft", "assault", "robbery", "burglary", "drug", "weapon"]
            for kw in common_keywords:
                if kw in question_lower:
                    keyword = kw
                    break
                    
            if keyword:
                try:
                    offense_result = await client.execute_tool(
                        "idoc", "get_offense_breakdown", {"keyword": keyword}
                    )
                    statistics["offense_breakdown"] = offense_result
                    traces.append(f"IDOC: Found {offense_result['total_people']} people with crimes matching '{keyword}'")
                except Exception as e:
                    errors.append(f"IDOC offense breakdown failed: {e}")
                    
            if intent_val == "cross_agency":
                # Cross-agency root: pull ALL insight IDs to build ephemeral graph
                try:
                    result = await client.execute_tool("idoc", "get_all_insight_ids", {})
                    ids = result if isinstance(result, list) else result.get("insight_ids", [])
                    incarcerated_ids.update(ids)
                    traces.append(f"IDOC bulk distinct pull: {len(ids)} unique insight_ids found")
                except Exception as e:
                    errors.append(f"IDOC get_all_insight_ids lookup failed: {e}")
                    logger.error(f"IDOC get_all_insight_ids lookup failed: {e}")
            
            elif intent_val not in ("statistics", "single_agency", "lookup"):
                # General fallback: pull active offender list
                try:
                    result = await client.execute_tool(
                        "idoc", "get_active_offenders", {"limit": 1000}
                    )
                    all_inmates = result if isinstance(result, list) else result.get("offenders", [])
                    for r in all_inmates:
                        if isinstance(r, dict) and r.get("insight_id"):
                            incarcerated_ids.add(r["insight_id"])
                    traces.append(f"IDOC active offenders (general): {len(all_inmates)} found")
                except Exception as e:
                    errors.append(f"IDOC general lookup failed: {e}")
                    logger.error(f"IDOC general lookup failed: {e}")

            idoc_data = {
                "incarcerated_ids": list(incarcerated_ids),  # key expected by CrossAgencyReasoner
                "inmates": all_inmates,
                "statistics": statistics,
            }
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


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────

def _extract_query_params_idhw(question: str) -> dict[str, Any]:
    params: dict[str, Any] = {}
    question_lower = question.lower()
    if "name" in question_lower or "named" in question_lower:
        params["include_names"] = True
    if "address" in question_lower:
        params["include_addresses"] = True
    return params


def _extract_query_params_idjc(question: str) -> dict[str, Any]:
    params: dict[str, Any] = {}
    question_lower = question.lower()
    if "current" in question_lower or "active" in question_lower:
        params["active_only"] = True
    if "offense" in question_lower or "charge" in question_lower:
        params["include_offenses"] = True
    return params


def _extract_query_params_idoc(question: str) -> dict[str, Any]:
    params: dict[str, Any] = {}
    question_lower = question.lower()
    if "current" in question_lower or "active" in question_lower:
        params["active_only"] = True
    if "sentence" in question_lower:
        params["include_sentencing"] = True
    if "facility" in question_lower or "location" in question_lower:
        params["include_facility"] = True
    return params
