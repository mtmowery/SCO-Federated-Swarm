"""
Async database query functions for IDOC agency module.

All functions use SQLAlchemy async API with asyncpg for non-blocking database access.
Functions return dicts for seamless JSON serialization.
"""

import logging
from typing import Any, Optional
from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database import get_pg_session
from .models import IDOCSentence

logger = logging.getLogger(__name__)

# Explicit set of statuses that represent active incarceration.
# Using a whitelist (not "!= DISCHARGED") avoids silently including new
# statuses added in the future (e.g. DECEASED, ESCAPED, FURLOUGH) that
# should NOT be counted as "incarcerated".
ACTIVE_INCARCERATION_STATUSES = frozenset({
    "ACTIVE",
    "SENTENCED",
    "COMMITTED",
    "REVOKED",
})


async def get_all_sentences(limit: int = 1000, offset: int = 0) -> list[dict]:
    """
    Retrieve all sentence records with pagination.

    Args:
        limit: Maximum number of records to return
        offset: Number of records to skip

    Returns:
        List of sentence records as dicts
    """
    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        stmt = select(IDOCSentence).limit(limit).offset(offset)
        result = await session.execute(stmt)
        records = result.scalars().all()

        return [record.to_dict() for record in records]


async def get_person_by_insight_id(insight_id: str) -> list[dict]:
    """
    Get all sentence records for a person by their insight_id.

    Args:
        insight_id: Cross-agency person identifier

    Returns:
        List of all sentence records for this person
    """
    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        stmt = select(IDOCSentence).where(IDOCSentence.insight_id == insight_id)
        result = await session.execute(stmt)
        records = result.scalars().all()

        return [record.to_dict() for record in records]


async def get_people_by_insight_ids(insight_ids: list[str]) -> dict[str, list[dict]]:
    """
    Bulk lookup: Get sentence records for multiple people.

    Args:
        insight_ids: List of cross-agency person identifiers

    Returns:
        Dict mapping insight_id to list of sentence records
    """
    if not insight_ids:
        return {}

    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        stmt = select(IDOCSentence).where(IDOCSentence.insight_id.in_(insight_ids))
        result = await session.execute(stmt)
        records = result.scalars().all()

        # Group by insight_id
        result_dict: dict[str, list[dict]] = {iid: [] for iid in insight_ids}
        for record in records:
            result_dict[record.insight_id].append(record.to_dict())

        return result_dict


async def check_incarceration(insight_ids: list[str]) -> dict[str, bool]:
    """
    Check which people have active (non-DISCHARGED) sentences.

    Args:
        insight_ids: List of cross-agency person identifiers

    Returns:
        Dict mapping insight_id to bool (True if has active sentence)
    """
    if not insight_ids:
        return {}

    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        # Active sentences: only explicitly active statuses (whitelist, not "!= DISCHARGED")
        stmt = select(IDOCSentence).where(
            and_(
                IDOCSentence.insight_id.in_(insight_ids),
                IDOCSentence.sent_status.in_(ACTIVE_INCARCERATION_STATUSES),
            )
        )
        result = await session.execute(stmt)
        records = result.scalars().all()
        
        # Get active insight IDs
        active_ids = {record.insight_id for record in records}
        
        # Return boolean dict for all requested IDs
        return {iid: (iid in active_ids) for iid in insight_ids}


async def count_incarcerated_from_ids(insight_ids: list[str]) -> int:
    """
    Count how many people from a list have active sentences.

    Args:
        insight_ids: List of cross-agency person identifiers

    Returns:
        Count of people with active sentences
    """
    if not insight_ids:
        return 0

    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        # Count distinct insight_ids with explicitly active statuses
        stmt = select(func.count(func.distinct(IDOCSentence.insight_id))).where(
            and_(
                IDOCSentence.insight_id.in_(insight_ids),
                IDOCSentence.sent_status.in_(ACTIVE_INCARCERATION_STATUSES),
            )
        )
        result = await session.execute(stmt)
        count = result.scalar()

        return count or 0


async def get_active_offenders(limit: int = 1000, offset: int = 0) -> list[dict]:
    """
    Get all offenders with non-discharged sentence status.

    Args:
        limit: Maximum number of records to return
        offset: Number of records to skip

    Returns:
        List of active offender sentence records
    """
    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        stmt = (
            select(IDOCSentence)
            .where(IDOCSentence.sent_status.in_(ACTIVE_INCARCERATION_STATUSES))
            .limit(limit)
            .offset(offset)
        )
        result = await session.execute(stmt)
        records = result.scalars().all()

        return [record.to_dict() for record in records]


async def get_offense_summary(keyword: Optional[str] = None) -> dict[str, int]:
    """
    Aggregate sentence counts by crime group or specific offense.

    Args:
        keyword: Optional offense descriptive string to match via ILIKE

    Returns:
        Dict mapping offense description to count of distinct people
    """
    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        # If keyword provided, group by the specific long desc, else group by crime group
        group_col = IDOCSentence.off_ldesc if keyword else IDOCSentence.crm_grp_desc
        stmt = select(
            group_col, func.count(func.distinct(IDOCSentence.insight_id)).label("count")
        )
        if keyword:
            stmt = stmt.where(IDOCSentence.off_ldesc.ilike(f"%{keyword}%"))
            
        stmt = stmt.group_by(group_col)
        result = await session.execute(stmt)
        rows = result.all()

        return {row[0] or "Unknown": row[1] for row in rows}


async def count_by_status() -> dict[str, int]:
    """
    Count sentences grouped by sentence status.

    Returns:
        Dict mapping sent_status to count
    """
    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        stmt = select(
            IDOCSentence.sent_status, func.count(IDOCSentence.id).label("count")
        ).group_by(IDOCSentence.sent_status)
        result = await session.execute(stmt)
        rows = result.all()

        return {row[0] or "Unknown": row[1] for row in rows}


async def search_sentences(filters: dict[str, Any]) -> list[dict]:
    """
    Flexible sentence search with multiple filter options.

    Supported filters:
    - insight_id: str - exact match
    - crm_grp_desc: str - exact match
    - sent_status: str - exact match
    - mitt_status: str - exact match
    - cnty_sdesc: str - case-insensitive substring
    - date_from: str (YYYY-MM-DD) - sent_beg_dtd >= date
    - date_to: str (YYYY-MM-DD) - sent_beg_dtd <= date

    Args:
        filters: Dict of filter criteria

    Returns:
        List of matching sentence records
    """
    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        conditions = []

        # Exact match filters
        if "insight_id" in filters:
            conditions.append(IDOCSentence.insight_id == filters["insight_id"])

        if "crm_grp_desc" in filters:
            conditions.append(IDOCSentence.crm_grp_desc == filters["crm_grp_desc"])

        if "sent_status" in filters:
            conditions.append(IDOCSentence.sent_status == filters["sent_status"])

        if "mitt_status" in filters:
            conditions.append(IDOCSentence.mitt_status == filters["mitt_status"])

        # Case-insensitive substring filters
        if "cnty_sdesc" in filters:
            conditions.append(IDOCSentence.cnty_sdesc.ilike(f"%{filters['cnty_sdesc']}%"))

        # Date range filters (string comparison works for YYYY-MM-DD)
        if "date_from" in filters:
            conditions.append(IDOCSentence.sent_beg_dtd >= filters["date_from"])

        if "date_to" in filters:
            conditions.append(IDOCSentence.sent_beg_dtd <= filters["date_to"])

        # Build query
        stmt = select(IDOCSentence)
        if conditions:
            stmt = stmt.where(and_(*conditions))

        result = await session.execute(stmt)
        records = result.scalars().all()

        return [record.to_dict() for record in records]


async def count_total_people() -> int:
    """
    Count the total number of unique people (insight_ids) in the IDOC database.

    Returns:
        Count of distinct insight_ids
    """
    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        stmt = select(func.count(func.distinct(IDOCSentence.insight_id)))
        result = await session.execute(stmt)
        count = result.scalar()

        return count or 0


async def get_all_insight_ids() -> list[str]:
    """
    Get all unique insight_ids in the adult corrections system.

    Returns:
        List of insight_id strings
    """
    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        stmt = select(IDOCSentence.insight_id).distinct().where(IDOCSentence.insight_id.is_not(None))
        result = await session.execute(stmt)
        return list(result.scalars().all())
