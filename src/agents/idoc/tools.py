"""
FastMCP tool server for IDOC agency module.

Exposes IDOC query capabilities as MCP tools for LLM agent consumption.
Tools are auto-discoverable by agents for natural language tool calling.
"""

import logging
from typing import Any
from fastmcp import FastMCP

from . import db

logger = logging.getLogger(__name__)

# Initialize MCP server for IDOC
mcp = FastMCP("idoc")


@mcp.tool()
async def get_sentences(limit: int = 100, offset: int = 0) -> dict[str, Any]:
    """
    Retrieve IDOC sentence records with pagination.

    Args:
        limit: Maximum records to return (default: 100, max: 1000)
        offset: Number of records to skip (default: 0)

    Returns:
        Dict with sentences list and pagination info
    """
    limit = min(limit, 1000)  # Cap at 1000
    sentences = await db.get_all_sentences(limit=limit, offset=offset)

    return {"count": len(sentences), "limit": limit, "offset": offset, "sentences": sentences}


@mcp.tool()
async def get_person(insight_id: str) -> dict[str, Any]:
    """
    Get all sentence records for a specific person by insight_id.

    Args:
        insight_id: Cross-agency person identifier (IDHW/IDJC/IDOC linkage)

    Returns:
        Dict with person's IDOC sentence records
    """
    sentences = await db.get_person_by_insight_id(insight_id)

    return {
        "insight_id": insight_id,
        "record_count": len(sentences),
        "sentences": sentences,
    }


@mcp.tool()
async def get_people_bulk(insight_ids: list[str]) -> dict[str, Any]:
    """
    Bulk lookup sentence records for multiple people.

    Args:
        insight_ids: List of cross-agency person identifiers

    Returns:
        Dict mapping insight_id to sentence records
    """
    results = await db.get_people_by_insight_ids(insight_ids)

    total_records = sum(len(v) for v in results.values())

    return {
        "query_count": len(insight_ids),
        "found_count": len([k for k, v in results.items() if v]),
        "total_records": total_records,
        "results": results,
    }


@mcp.tool()
async def check_incarceration(insight_ids: list[str]) -> dict[str, Any]:
    """
    Check which people have active (non-discharged) sentences in IDOC.

    Args:
        insight_ids: List of cross-agency person identifiers

    Returns:
        Dict mapping insight_id to boolean (True = currently incarcerated)
    """
    incarceration_status = await db.check_incarceration(insight_ids)

    active_count = sum(1 for v in incarceration_status.values() if v)

    return {
        "query_count": len(insight_ids),
        "incarcerated_count": active_count,
        "status": incarceration_status,
    }


@mcp.tool()
async def count_incarcerated(insight_ids: list[str]) -> dict[str, Any]:
    """
    Count how many people from a list have active sentences.

    Args:
        insight_ids: List of cross-agency person identifiers

    Returns:
        Dict with incarceration count
    """
    count = await db.count_incarcerated_from_ids(insight_ids)

    return {
        "query_count": len(insight_ids),
        "incarcerated_count": count,
        "percentage": round(100.0 * count / len(insight_ids), 2) if insight_ids else 0.0,
    }


@mcp.tool()
async def get_active_offenders(limit: int = 100, offset: int = 0) -> dict[str, Any]:
    """
    Get list of all active offenders (non-discharged sentences).

    Args:
        limit: Maximum records to return (default: 100, max: 1000)
        offset: Number of records to skip (default: 0)

    Returns:
        Dict with active offender records
    """
    limit = min(limit, 1000)
    offenders = await db.get_active_offenders(limit=limit, offset=offset)

    return {
        "count": len(offenders),
        "limit": limit,
        "offset": offset,
        "offenders": offenders,
    }


@mcp.tool()
async def get_offense_summary() -> dict[str, Any]:
    """
    Get aggregate statistics of sentences by crime group.

    Returns aggregate counts for each crime group/type in IDOC.

    Returns:
        Dict with crime_group_desc mapped to sentence counts
    """
    summary = await db.get_offense_summary()
    total = sum(summary.values())

    return {
        "total_sentences": total,
        "crime_groups": len(summary),
        "by_group": summary,
    }


@mcp.tool()
async def count_by_status() -> dict[str, Any]:
    """
    Get sentence counts grouped by sentence status.

    Returns breakdown of sentence statuses (ACTIVE, DISCHARGED, etc).

    Returns:
        Dict with sent_status mapped to counts
    """
    counts = await db.count_by_status()
    total = sum(counts.values())

    return {"total_sentences": total, "by_status": counts}


@mcp.tool()
async def search_sentences(filters: dict[str, Any]) -> dict[str, Any]:
    """
    Advanced search for sentences with flexible filtering.

    Supported filters:
    - insight_id: Cross-agency person ID
    - ofndr_num: IDOC offender number
    - fnam: First name (substring, case-insensitive)
    - lnam: Last name (substring, case-insensitive)
    - crm_grp_desc: Crime group description
    - sent_status: Sentence status (e.g., ACTIVE, DISCHARGED)
    - mitt_status: Mittimus status
    - cnty_sdesc: County description (substring, case-insensitive)
    - sex_cd: Sex code
    - date_from: Sentence start date from (YYYY-MM-DD)
    - date_to: Sentence start date to (YYYY-MM-DD)

    Args:
        filters: Dict of filter criteria

    Returns:
        Dict with matching sentence records
    """
    sentences = await db.search_sentences(filters)

    return {
        "filter_count": len(filters),
        "result_count": len(sentences),
        "filters": filters,
        "sentences": sentences,
    }
