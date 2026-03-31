"""FastMCP tool definitions for IDJC (Idaho Department of Juvenile Corrections).

Exposes IDJC query capabilities as MCP tools with proper type hints and docstrings.
"""

import logging
from typing import Any, Optional
from datetime import date

from fastmcp import FastMCP

from . import db

logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("idjc")


@mcp.tool()
async def get_commitments(
    limit: int = 1000, offset: int = 0
) -> dict[str, Any]:
    """
    Get all IDJC commitments with pagination.

    This tool retrieves commitment records from the IDJC database with
    optional pagination support.

    Args:
        limit: Maximum number of records to return (default: 1000)
        offset: Number of records to skip (default: 0)

    Returns:
        Dictionary with 'commitments' list and 'count' of results
    """
    try:
        commitments = await db.get_all_commitments(limit=limit, offset=offset)
        logger.info(f"Retrieved {len(commitments)} commitments")
        return {
            "commitments": commitments,
            "count": len(commitments),
            "limit": limit,
            "offset": offset,
        }
    except Exception as e:
        logger.error(f"Error retrieving commitments: {e}")
        return {"error": str(e), "commitments": []}


@mcp.tool()
async def get_person(insight_id: str) -> dict[str, Any]:
    """
    Get all commitment records for a person by insight_id.

    Retrieves the complete commitment history for a specific individual
    identified by their global insight_id.

    Args:
        insight_id: The global insight identifier for the person

    Returns:
        Dictionary with 'commitments' list for that person
    """
    try:
        if not insight_id:
            return {"error": "insight_id is required", "commitments": []}

        commitments = await db.get_person_by_insight_id(insight_id)
        logger.info(
            f"Retrieved {len(commitments)} commitments for insight_id: {insight_id}"
        )
        return {
            "insight_id": insight_id,
            "commitments": commitments,
            "count": len(commitments),
        }
    except Exception as e:
        logger.error(f"Error retrieving person {insight_id}: {e}")
        return {"error": str(e), "commitments": []}


@mcp.tool()
async def get_people_bulk(insight_ids: list[str]) -> dict[str, Any]:
    """
    Bulk lookup: get commitment records for multiple insight_ids.

    Efficiently retrieves commitment records for multiple individuals
    in a single query.

    Args:
        insight_ids: List of insight identifiers to look up

    Returns:
        Dictionary mapping insight_id to list of commitment records
    """
    try:
        if not insight_ids:
            return {"error": "insight_ids list is required", "results": {}}

        results = await db.get_people_by_insight_ids(insight_ids)
        total_records = sum(len(records) for records in results.values())
        logger.info(f"Bulk lookup returned {total_records} records for {len(results)} people")
        return {
            "results": results,
            "count": len(results),
            "total_records": total_records,
        }
    except Exception as e:
        logger.error(f"Error in bulk lookup: {e}")
        return {"error": str(e), "results": {}}


@mcp.tool()
async def get_active_commitments(
    limit: int = 1000, offset: int = 0
) -> dict[str, Any]:
    """
    Get all active commitments (status='Active').

    Retrieves only commitments that are currently active, with pagination support.

    Args:
        limit: Maximum number of records to return (default: 1000)
        offset: Number of records to skip (default: 0)

    Returns:
        Dictionary with 'commitments' list and 'count' of active records
    """
    try:
        commitments = await db.get_active_commitments(limit=limit, offset=offset)
        logger.info(f"Retrieved {len(commitments)} active commitments")
        return {
            "commitments": commitments,
            "count": len(commitments),
            "limit": limit,
            "offset": offset,
        }
    except Exception as e:
        logger.error(f"Error retrieving active commitments: {e}")
        return {"error": str(e), "commitments": []}


@mcp.tool()
async def check_juvenile_record(insight_ids: list[str]) -> dict[str, Any]:
    """
    Check which insight_ids have juvenile records in IDJC.

    Performs a bulk check to determine which individuals have active or
    historical juvenile commitments.

    Args:
        insight_ids: List of insight identifiers to check

    Returns:
        Dictionary mapping insight_id to boolean indicating presence of record
    """
    try:
        if not insight_ids:
            return {"error": "insight_ids list is required", "results": {}}

        results = await db.check_juvenile_record(insight_ids)
        has_record_count = sum(1 for v in results.values() if v)
        logger.info(
            f"Juvenile record check: {has_record_count}/{len(results)} have records"
        )
        return {
            "results": results,
            "total_checked": len(results),
            "with_records": has_record_count,
        }
    except Exception as e:
        logger.error(f"Error checking juvenile records: {e}")
        return {"error": str(e), "results": {}}


@mcp.tool()
async def get_offense_summary() -> dict[str, Any]:
    """
    Get aggregate counts by offense category.

    Returns summary statistics of commitments grouped by offense category,
    useful for understanding the distribution of offense types in IDJC.

    Returns:
        Dictionary mapping offense_category to count
    """
    try:
        summary = await db.get_offense_summary()
        total = sum(summary.values())
        logger.info(f"Offense summary: {len(summary)} categories, {total} total records")
        return {
            "summary": summary,
            "category_count": len(summary),
            "total_records": total,
        }
    except Exception as e:
        logger.error(f"Error retrieving offense summary: {e}")
        return {"error": str(e), "summary": {}}


@mcp.tool()
async def count_by_status() -> dict[str, Any]:
    """
    Get commitment counts grouped by status.

    Returns summary of commitments by their current status (Active, Released, etc),
    providing overview of the IDJC population.

    Returns:
        Dictionary mapping status to count
    """
    try:
        counts = await db.count_by_status()
        total = sum(counts.values())
        logger.info(f"Status counts: {len(counts)} statuses, {total} total records")
        return {
            "counts": counts,
            "status_count": len(counts),
            "total_records": total,
        }
    except Exception as e:
        logger.error(f"Error retrieving status counts: {e}")
        return {"error": str(e), "counts": {}}


@mcp.tool()
async def search_commitments(
    insight_id: Optional[str] = None,
    ijos_id: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    ssn: Optional[str] = None,
    status: Optional[str] = None,
    offense_category: Optional[str] = None,
    offense_level: Optional[str] = None,
    committing_county: Optional[str] = None,
    significance_level: Optional[str] = None,
    dob_start: Optional[str] = None,
    dob_end: Optional[str] = None,
    commitment_start: Optional[str] = None,
    commitment_end: Optional[str] = None,
    limit: int = 1000,
    offset: int = 0,
) -> dict[str, Any]:
    """
    Search commitments with flexible filtering.

    Supports multiple filter criteria combined with AND logic.
    Dates should be provided in ISO 8601 format (YYYY-MM-DD).

    Args:
        insight_id: Filter by insight_id (exact match)
        ijos_id: Filter by IJOS ID (exact match)
        first_name: Filter by first name (substring match)
        last_name: Filter by last name (substring match)
        ssn: Filter by Social Security Number (exact match)
        status: Filter by commitment status (exact match)
        offense_category: Filter by offense category (exact match)
        offense_level: Filter by offense level (exact match)
        committing_county: Filter by county (exact match)
        significance_level: Filter by significance level (exact match)
        dob_start: Start date for date of birth range (ISO format)
        dob_end: End date for date of birth range (ISO format)
        commitment_start: Start date for commitment date range (ISO format)
        commitment_end: End date for commitment date range (ISO format)
        limit: Maximum number of results (default: 1000)
        offset: Number of results to skip (default: 0)

    Returns:
        Dictionary with 'commitments' list and 'count'
    """
    try:
        # Build filters dictionary
        filters = {}

        if insight_id:
            filters["insight_id"] = insight_id
        if ijos_id:
            filters["ijos_id"] = ijos_id
        if first_name:
            filters["first_name"] = first_name
        if last_name:
            filters["last_name"] = last_name
        if ssn:
            filters["ssn"] = ssn
        if status:
            filters["status"] = status
        if offense_category:
            filters["offense_category"] = offense_category
        if offense_level:
            filters["offense_level"] = offense_level
        if committing_county:
            filters["committing_county"] = committing_county
        if significance_level:
            filters["significance_level"] = significance_level
        if dob_start:
            filters["dob_start"] = dob_start
        if dob_end:
            filters["dob_end"] = dob_end
        if commitment_start:
            filters["commitment_start"] = commitment_start
        if commitment_end:
            filters["commitment_end"] = commitment_end

        filters["limit"] = limit
        filters["offset"] = offset

        commitments = await db.search_commitments(filters)
        logger.info(f"Search returned {len(commitments)} results")
        return {
            "commitments": commitments,
            "count": len(commitments),
            "limit": limit,
            "offset": offset,
        }
    except Exception as e:
        logger.error(f"Error searching commitments: {e}")
        return {"error": str(e), "commitments": []}
