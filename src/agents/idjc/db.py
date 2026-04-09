"""Database query functions for IDJC commitment records.

Provides async query interface to PostgreSQL with comprehensive lookup,
filtering, aggregation, and bulk operations.
"""

import logging
from typing import Optional, Any
from datetime import date

from sqlalchemy import select, func, and_, or_, desc
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database import pg_session_context
from .models import IDJCCommitment

logger = logging.getLogger(__name__)


async def get_all_commitments(
    limit: int = 1000, offset: int = 0
) -> list[dict[str, Any]]:
    """
    Get all IDJC commitments with pagination.

    Args:
        limit: Maximum number of records to return
        offset: Number of records to skip

    Returns:
        List of commitment records as dictionaries
    """
    async with pg_session_context("idjc") as session:
        query = select(IDJCCommitment).offset(offset).limit(limit)
        result = await session.execute(query)
        commitments = result.scalars().all()
        return [c.to_dict() for c in commitments]


async def get_person_by_insight_id(insight_id: str) -> list[dict[str, Any]]:
    """
    Get all commitment records for a person by insight_id.

    Args:
        insight_id: The global insight identifier

    Returns:
        List of all commitment records for this person
    """
    async with pg_session_context("idjc") as session:
        query = select(IDJCCommitment).where(
            IDJCCommitment.insight_id == insight_id
        ).order_by(desc(IDJCCommitment.date_of_commitment))
        result = await session.execute(query)
        commitments = result.scalars().all()
        return [c.to_dict() for c in commitments]


async def get_people_by_insight_ids(insight_ids: list[str]) -> dict[str, list[dict]]:
    """
    Bulk lookup: get commitment records for multiple insight_ids.

    Args:
        insight_ids: List of insight identifiers

    Returns:
        Dictionary mapping insight_id to list of commitment records
    """
    if not insight_ids:
        return {}

    async with pg_session_context("idjc") as session:
        query = select(IDJCCommitment).where(
            IDJCCommitment.insight_id.in_(insight_ids)
        ).order_by(IDJCCommitment.insight_id, desc(IDJCCommitment.date_of_commitment))
        result = await session.execute(query)
        commitments = result.scalars().all()

        # Organize by insight_id
        result_dict: dict[str, list[dict]] = {
            insight_id: [] for insight_id in insight_ids
        }
        for commitment in commitments:
            result_dict[commitment.insight_id].append(commitment.to_dict())

        return result_dict


async def get_active_commitments(
    limit: int = 1000, offset: int = 0
) -> list[dict[str, Any]]:
    """
    Get all active commitments (status='Active').

    Args:
        limit: Maximum number of records to return
        offset: Number of records to skip

    Returns:
        List of active commitment records
    """
    async with pg_session_context("idjc") as session:
        query = (
            select(IDJCCommitment)
            .where(IDJCCommitment.status == "Active")
            .order_by(desc(IDJCCommitment.date_of_commitment))
            .offset(offset)
            .limit(limit)
        )
        result = await session.execute(query)
        commitments = result.scalars().all()
        return [c.to_dict() for c in commitments]


async def get_commitments_by_county(
    county: str, limit: int = 1000, offset: int = 0
) -> list[dict[str, Any]]:
    """
    Get commitments for a specific county.

    Args:
        county: County name
        limit: Maximum number of records to return
        offset: Number of records to skip

    Returns:
        List of commitment records from that county
    """
    async with pg_session_context("idjc") as session:
        query = (
            select(IDJCCommitment)
            .where(IDJCCommitment.committing_county == county)
            .order_by(desc(IDJCCommitment.date_of_commitment))
            .offset(offset)
            .limit(limit)
        )
        result = await session.execute(query)
        commitments = result.scalars().all()
        return [c.to_dict() for c in commitments]


async def get_offense_summary(keyword: Optional[str] = None) -> dict[str, int]:
    """
    Get aggregate counts by offense category or specific offense description.

    Args:
        keyword: Optional descriptive keyword mapped to ILIKE filters

    Returns:
        Dictionary mapping offense classification to distinct people insight_ids count
    """
    async with pg_session_context("idjc") as session:
        group_col = IDJCCommitment.offense_description if keyword else IDJCCommitment.offense_category
        
        query = select(group_col, func.count(func.distinct(IDJCCommitment.insight_id)))
        
        if keyword:
            query = query.where(IDJCCommitment.offense_description.ilike(f"%{keyword}%"))
        else:
            query = query.where(group_col.isnot(None))
            
        query = query.group_by(group_col).order_by(desc(func.count(func.distinct(IDJCCommitment.insight_id))))
        
        result = await session.execute(query)
        rows = result.all()
        return {classification or "Unknown": count for classification, count in rows}


async def count_by_status() -> dict[str, int]:
    """
    Get commitment counts grouped by status.

    Returns:
        Dictionary mapping status to count
    """
    async with pg_session_context("idjc") as session:
        query = (
            select(IDJCCommitment.status, func.count(IDJCCommitment.id))
            .group_by(IDJCCommitment.status)
            .order_by(desc(func.count(IDJCCommitment.id)))
        )
        result = await session.execute(query)
        rows = result.all()
        return {status: count for status, count in rows}


async def get_top_offenders(limit: int = 10) -> list[dict[str, Any]]:
    """
    Get top individuals in IDJC with the most offenses.

    Args:
        limit: Max number of people to return.

    Returns:
        List of dictionaries containing insight_id and offense_count.
    """
    async with pg_session_context("idjc") as session:
        query = (
            select(IDJCCommitment.insight_id, func.count(IDJCCommitment.id).label("offense_count"))
            .group_by(IDJCCommitment.insight_id)
            .order_by(desc("offense_count"))
            .limit(limit)
        )
        result = await session.execute(query)
        rows = result.all()
        return [{"insight_id": insight_id, "offense_count": count} for insight_id, count in rows]


async def check_juvenile_record(insight_ids: list[str]) -> dict[str, bool]:
    """
    Check which insight_ids have juvenile records in IDJC.

    Args:
        insight_ids: List of insight identifiers to check

    Returns:
        Dictionary mapping insight_id to boolean (has_record)
    """
    if not insight_ids:
        return {}

    async with pg_session_context("idjc") as session:
        # Get distinct insight_ids that exist in IDJC
        query = (
            select(IDJCCommitment.insight_id)
            .where(IDJCCommitment.insight_id.in_(insight_ids))
            .distinct()
        )
        result = await session.execute(query)
        existing_ids = set(row[0] for row in result.all())

        # Return dictionary with all IDs
        return {insight_id: insight_id in existing_ids for insight_id in insight_ids}


async def search_commitments(filters: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Flexible search on commitments with multiple filter options.

    Supported filters:
    - insight_id: str
    - status: str
    - offense_category: str
    - offense_level: str
    - committing_county: str
    - commitment_start: str
    - commitment_end: str
    - significance_level: str
    - limit: int (default 1000)
    - offset: int (default 0)

    Args:
        filters: Dictionary of filter criteria

    Returns:
        List of matching commitment records
    """
    limit = filters.pop("limit", 1000)
    offset = filters.pop("offset", 0)

    async with pg_session_context("idjc") as session:
        query = select(IDJCCommitment)
        conditions = []

        if "insight_id" in filters:
            conditions.append(IDJCCommitment.insight_id == filters["insight_id"])

        if "status" in filters:
            conditions.append(IDJCCommitment.status == filters["status"])

        if "offense_category" in filters:
            conditions.append(
                IDJCCommitment.offense_category == filters["offense_category"]
            )

        if "offense_level" in filters:
            conditions.append(IDJCCommitment.offense_level == filters["offense_level"])

        if "committing_county" in filters:
            conditions.append(
                IDJCCommitment.committing_county == filters["committing_county"]
            )

        if "significance_level" in filters:
            conditions.append(
                IDJCCommitment.significance_level == filters["significance_level"]
            )

        # Date range filters

        if "commitment_start" in filters:
            conditions.append(IDJCCommitment.date_of_commitment >= filters["commitment_start"])

        if "commitment_end" in filters:
            conditions.append(IDJCCommitment.date_of_commitment <= filters["commitment_end"])

        # Apply all conditions with AND logic
        if conditions:
            query = query.where(and_(*conditions))

        # Sort and paginate
        query = (
            query.order_by(desc(IDJCCommitment.date_of_commitment))
            .offset(offset)
            .limit(limit)
        )

        result = await session.execute(query)
        commitments = result.scalars().all()
        return [c.to_dict() for c in commitments]


async def count_total_people() -> int:
    """
    Count the total number of unique people (insight_ids) in the IDJC database.

    Returns:
        Count of distinct insight_ids
    """
    async with pg_session_context("idjc") as session:
        query = select(func.count(func.distinct(IDJCCommitment.insight_id)))
        result = await session.execute(query)
        count = result.scalar()
        return count or 0


async def get_all_insight_ids() -> list[str]:
    """
    Get all unique insight_ids in the juvenile corrections system.

    Returns:
        List of insight_id strings
    """
    async with pg_session_context("idjc") as session:
        query = select(IDJCCommitment.insight_id).distinct().where(IDJCCommitment.insight_id.is_not(None))
        result = await session.execute(query)
        return list(result.scalars().all())
