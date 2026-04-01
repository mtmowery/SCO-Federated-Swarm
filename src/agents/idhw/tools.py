"""MCP tool definitions for IDHW agency operations.

Registers all IDHW query and analysis capabilities as FastMCP tools
for use by LLM agents and other components.
"""

import logging
from typing import Any, Optional

from fastmcp import FastMCP

from . import db

logger = logging.getLogger(__name__)

# Create FastMCP server instance for IDHW
mcp = FastMCP("idhw")


@mcp.tool()
async def get_children() -> dict[str, Any]:
    """Get all children records from IDHW foster care system.

    Returns:
        Dictionary with 'children' key containing list of child records.
        Each record includes person details, family relationships, and care dates.
    """
    children = await db.get_all_children()
    return {"children": children, "count": len(children)}


@mcp.tool()
async def get_foster_children() -> dict[str, Any]:
    """Get all children currently or previously in foster care.

    Returns children with non-null start_care_date.

    Returns:
        Dictionary with 'foster_children' key and count.
        Each record includes care dates and end reasons where applicable.
    """
    foster_children = await db.get_foster_children()
    return {"foster_children": foster_children, "count": len(foster_children)}


@mcp.tool()
async def get_person(insight_id: str) -> dict[str, Any]:
    """Get a single person record by insight_id.

    Args:
        insight_id: Global identity identifier (IDHW insight_id)

    Returns:
        Dictionary with 'person' key containing the person record if found,
        or None if not found.
    """
    person = await db.get_person_by_insight_id(insight_id)
    return {"person": person, "found": person is not None}


@mcp.tool()
async def get_people_bulk(insight_ids: list[str]) -> dict[str, Any]:
    """Get multiple people records by insight_ids (bulk lookup).

    Args:
        insight_ids: List of insight_id strings to look up

    Returns:
        Dictionary with 'people' key containing list of found records.
        Records not found are simply omitted from results.
    """
    people = await db.get_people_by_insight_ids(insight_ids)
    return {"people": people, "count": len(people), "requested": len(insight_ids)}


@mcp.tool()
async def get_family_relationships() -> dict[str, Any]:
    """Get child-parent relationships (insight_ids only).

    Returns structured family relationships for all children in the system.
    Useful for identity linkage and relationship mapping.

    Returns:
        Dictionary with 'relationships' key containing list of relationships.
        Each relationship has child_insight_id, mother_insight_id, father_insight_id.
    """
    relationships = await db.get_family_relationships()
    return {"relationships": relationships, "total_children": len(relationships)}


@mcp.tool()
async def get_parent_map() -> dict[str, Any]:
    """Get mapping of parent insight_ids to their children.

    Enables quick lookup of all children for a given parent.

    Returns:
        Dictionary mapping parent insight_id -> list of child insight_ids.
        Includes both mothers and fathers.
    """
    parent_map = await db.get_parent_map()
    return {
        "parent_map": parent_map,
        "total_parents": len(parent_map),
        "total_parent_child_links": sum(len(children) for children in parent_map.values()),
    }


@mcp.tool()
async def count_by_end_reason() -> dict[str, Any]:
    """Get aggregate counts of children by end_reason.

    Shows how many children exited foster care for each reason
    (adoption, reunification, etc.).

    Returns:
        Dictionary with counts keyed by end_reason string.
        Includes total count and most common reasons.
    """
    counts = await db.count_children_by_end_reason()
    total = sum(counts.values())
    return {
        "counts_by_reason": counts,
        "total_children_with_end_reason": total,
        "most_common_reason": max(counts.items(), key=lambda x: x[1])[0]
        if counts
        else None,
    }


@mcp.tool()
async def search_people(
    insight_id: Optional[str] = None,
    child_insight_id: Optional[str] = None,
    mother_insight_id: Optional[str] = None,
    father_insight_id: Optional[str] = None,
    person_type: Optional[str] = None,
    gender: Optional[str] = None,
    end_reason: Optional[str] = None,
) -> dict[str, Any]:
    """Search for people using flexible filters.

    Can search by:
    - insight_id: Exact match on universal ID
    - child_insight_id: Exact match on child ID
    - mother_insight_id: Exact match on mother ID
    - father_insight_id: Exact match on father ID
    - person_type: 'child', 'mother', 'father'
    - gender: 'M', 'F', etc.
    - end_reason: Exact end reason match

    Args:
        insight_id: Exact match on global insight ID
        child_insight_id: Exact match on child insight ID
        mother_insight_id: Exact match on mother insight ID
        father_insight_id: Exact match on father insight ID
        person_type: Filter by person type
        gender: Filter by gender
        end_reason: Filter by end reason

    Returns:
        Dictionary with 'results' key containing matching records and count.
    """
    filters = {}

    if insight_id is not None:
        filters["insight_id"] = insight_id
    if child_insight_id is not None:
        filters["child_insight_id"] = child_insight_id
    if mother_insight_id is not None:
        filters["mother_insight_id"] = mother_insight_id
    if father_insight_id is not None:
        filters["father_insight_id"] = father_insight_id

    if person_type is not None:
        filters["person_type"] = person_type
    if gender is not None:
        filters["gender"] = gender
    if end_reason is not None:
        filters["end_reason"] = end_reason

    results = await db.search_people(filters)
    return {"results": results, "count": len(results)}


@mcp.tool()
async def get_stats() -> dict[str, Any]:
    """Get overall statistics about IDHW records.

    Provides aggregate counts for the entire foster care dataset.

    Returns:
        Dictionary with counts of total records, children, foster children,
        terminations of parental rights, and deaths.
    """
    stats = await db.get_stats()
    return {"statistics": stats}
