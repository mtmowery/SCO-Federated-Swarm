"""
Async database query functions for IDOC agency module.

All functions use SQLAlchemy async API with asyncpg for non-blocking database access.
Functions return dicts for seamless JSON serialization.
"""

import logging
from typing import Any, Optional
from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from src.shared.database import get_pg_session
from .models import IDOCSentence

logger = logging.getLogger(__name__)


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
        # Active sentences are those NOT discharged
        stmt = select(IDOCSentence).where(
            and_(
                IDOCSentence.insight_id.in_(insight_ids),
                IDOCSentence.sent_status != "DISCHARGED",
            )
        )
        result = await session.execute(stmt)
        records = result.scalars().all()

        # Get unique insight_ids with active sentences
        active_ids = {record.insight_id for record in records}

        # Return dict with all insight_ids marked True or False
        return {iid: iid in active_ids for iid in insight_ids}


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
        # Count distinct insight_ids with non-discharged status
        stmt = select(func.count(func.distinct(IDOCSentence.insight_id))).where(
            and_(
                IDOCSentence.insight_id.in_(insight_ids),
                IDOCSentence.sent_status != "DISCHARGED",
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
            .where(IDOCSentence.sent_status != "DISCHARGED")
            .limit(limit)
            .offset(offset)
        )
        result = await session.execute(stmt)
        records = result.scalars().all()

        return [record.to_dict() for record in records]


async def get_offense_summary() -> dict[str, int]:
    """
    Aggregate sentence counts by crime group.

    Returns:
        Dict mapping crime_group_desc to count of sentences
    """
    session_maker = await get_pg_session("idoc")

    async with session_maker() as session:
        stmt = select(
            IDOCSentence.crm_grp_desc, func.count(IDOCSentence.id).label("count")
        ).group_by(IDOCSentence.crm_grp_desc)
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
    - ofndr_num: str - exact match
    - lnam: str - case-insensitive substring
    - fnam: str - case-insensitive substring
    - crm_grp_desc: str - exact match
    - sent_status: str - exact match
    - mitt_status: str - exact match
    - cnty_sdesc: str - case-insensitive substring
    - sex_cd: str - exact match
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

        if "ofndr_num" in filters:
            conditions.append(IDOCSentence.ofndr_num == filters["ofndr_num"])

        if "crm_grp_desc" in filters:
            conditions.append(IDOCSentence.crm_grp_desc == filters["crm_grp_desc"])

        if "sent_status" in filters:
            conditions.append(IDOCSentence.sent_status == filters["sent_status"])

        if "mitt_status" in filters:
            conditions.append(IDOCSentence.mitt_status == filters["mitt_status"])

        if "sex_cd" in filters:
            conditions.append(IDOCSentence.sex_cd == filters["sex_cd"])

        # Case-insensitive substring filters
        if "lnam" in filters:
            conditions.append(IDOCSentence.lnam.ilike(f"%{filters['lnam']}%"))

        if "fnam" in filters:
            conditions.append(IDOCSentence.fnam.ilike(f"%{filters['fnam']}%"))

        if "cnty_sdesc" in filters:
            conditions.append(IDOCSentence.cnty_sdesc.ilike(f"%{filters['cnty_sdesc']}%"))

        # Date range filters
        if "date_from" in filters:
            from datetime import datetime

            date_from = datetime.fromisoformat(filters["date_from"]).date()
            conditions.append(IDOCSentence.sent_beg_dtd >= date_from)

        if "date_to" in filters:
            from datetime import datetime

            date_to = datetime.fromisoformat(filters["date_to"]).date()
            conditions.append(IDOCSentence.sent_beg_dtd <= date_to)

        # Build query
        stmt = select(IDOCSentence)
        if conditions:
            stmt = stmt.where(and_(*conditions))

        result = await session.execute(stmt)
        records = result.scalars().all()

        return [record.to_dict() for record in records]
