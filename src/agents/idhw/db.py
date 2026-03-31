"""Database query functions for IDHW foster care data.

Provides async database operations for person lookups, family relationships,
and aggregations.
"""

import logging
from typing import Optional, Any
from datetime import datetime

from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from ..shared.database import get_pg_session
from .models import IDHWPerson

logger = logging.getLogger(__name__)


async def get_all_children() -> list[dict[str, Any]]:
    """Get all records where person_type='child'.

    Returns:
        List of dictionaries representing child records
    """
    session_maker = await get_pg_session("idhw")

    async with session_maker() as session:
        stmt = select(IDHWPerson).where(IDHWPerson.person_type == "child")
        result = await session.execute(stmt)
        rows = result.scalars().all()
        return [row.to_dict() for row in rows]


async def get_person_by_insight_id(insight_id: str) -> Optional[dict[str, Any]]:
    """Get a single person by insight_id.

    Args:
        insight_id: Global identity identifier

    Returns:
        Dictionary representation of person record or None if not found
    """
    session_maker = await get_pg_session("idhw")

    async with session_maker() as session:
        stmt = select(IDHWPerson).where(IDHWPerson.insight_id == insight_id)
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        return row.to_dict() if row else None


async def get_people_by_insight_ids(insight_ids: list[str]) -> list[dict[str, Any]]:
    """Bulk lookup of people by insight_ids.

    Args:
        insight_ids: List of global identity identifiers

    Returns:
        List of dictionaries representing person records (in arbitrary order)
    """
    if not insight_ids:
        return []

    session_maker = await get_pg_session("idhw")

    async with session_maker() as session:
        stmt = select(IDHWPerson).where(IDHWPerson.insight_id.in_(insight_ids))
        result = await session.execute(stmt)
        rows = result.scalars().all()
        return [row.to_dict() for row in rows]


async def get_family_relationships() -> list[dict[str, Any]]:
    """Get child/mother/father insight_id relationships for all children.

    Returns:
        List of dictionaries with keys: child_insight_id, mother_insight_id,
        father_insight_id (for all children regardless of parent presence)
    """
    session_maker = await get_pg_session("idhw")

    async with session_maker() as session:
        stmt = select(
            IDHWPerson.child_insight_id,
            IDHWPerson.mother_insight_id,
            IDHWPerson.father_insight_id,
        ).where(IDHWPerson.person_type == "child")

        result = await session.execute(stmt)
        rows = result.all()
        return [
            {
                "child_insight_id": row[0],
                "mother_insight_id": row[1],
                "father_insight_id": row[2],
            }
            for row in rows
        ]


async def get_children_with_parent_ids() -> list[dict[str, Any]]:
    """Get children records with their parent insight_ids.

    Returns:
        List of dictionaries with child record + parent insight_ids
    """
    session_maker = await get_pg_session("idhw")

    async with session_maker() as session:
        stmt = select(IDHWPerson).where(IDHWPerson.person_type == "child")
        result = await session.execute(stmt)
        rows = result.scalars().all()
        return [row.to_dict() for row in rows]


async def get_foster_children() -> list[dict[str, Any]]:
    """Get children with start_care_date not null (in foster care).

    Returns:
        List of dictionaries representing foster care records
    """
    session_maker = await get_pg_session("idhw")

    async with session_maker() as session:
        stmt = select(IDHWPerson).where(
            and_(
                IDHWPerson.person_type == "child",
                IDHWPerson.start_care_date.isnot(None),
            )
        )
        result = await session.execute(stmt)
        rows = result.scalars().all()
        return [row.to_dict() for row in rows]


async def get_parent_map() -> dict[str, list[str]]:
    """Map parent insight_ids to lists of child insight_ids.

    Returns:
        Dictionary where keys are parent insight_ids (mother or father)
        and values are lists of child insight_ids
    """
    session_maker = await get_pg_session("idhw")

    parent_map: dict[str, list[str]] = {}

    async with session_maker() as session:
        # Get all children with parent relationships
        stmt = select(IDHWPerson.child_insight_id, IDHWPerson.mother_insight_id).where(
            and_(
                IDHWPerson.person_type == "child",
                IDHWPerson.mother_insight_id.isnot(None),
            )
        )
        result = await session.execute(stmt)
        rows = result.all()

        for child_id, mother_id in rows:
            if mother_id not in parent_map:
                parent_map[mother_id] = []
            parent_map[mother_id].append(child_id)

        # Get father relationships
        stmt = select(IDHWPerson.child_insight_id, IDHWPerson.father_insight_id).where(
            and_(
                IDHWPerson.person_type == "child",
                IDHWPerson.father_insight_id.isnot(None),
            )
        )
        result = await session.execute(stmt)
        rows = result.all()

        for child_id, father_id in rows:
            if father_id not in parent_map:
                parent_map[father_id] = []
            parent_map[father_id].append(child_id)

    return parent_map


async def count_children_by_end_reason() -> dict[str, int]:
    """Aggregate count of children by end_reason.

    Returns:
        Dictionary mapping end_reason strings to counts
    """
    session_maker = await get_pg_session("idhw")

    async with session_maker() as session:
        stmt = (
            select(IDHWPerson.end_reason, func.count(IDHWPerson.id))
            .where(IDHWPerson.person_type == "child")
            .group_by(IDHWPerson.end_reason)
        )
        result = await session.execute(stmt)
        rows = result.all()
        return {str(reason): count for reason, count in rows}


async def search_people(filters: dict[str, Any]) -> list[dict[str, Any]]:
    """Flexible search with filters on any column.

    Supports filtering on:
    - person_type: str
    - agency_id: str
    - first_name: str (contains, case-insensitive)
    - last_name: str (contains, case-insensitive)
    - dob: datetime or tuple (start, end) for range
    - start_care_date: datetime or tuple (start, end) for range
    - end_care_date: datetime or tuple (start, end) for range
    - gender: str
    - ssn: str (exact match)
    - end_reason: str

    Args:
        filters: Dictionary of filter criteria

    Returns:
        List of matching person records as dictionaries
    """
    session_maker = await get_pg_session("idhw")

    async with session_maker() as session:
        conditions = []

        # Build dynamic filters
        if "person_type" in filters:
            conditions.append(IDHWPerson.person_type == filters["person_type"])

        if "agency_id" in filters:
            conditions.append(IDHWPerson.agency_id == filters["agency_id"])

        if "first_name" in filters:
            conditions.append(
                IDHWPerson.first_name.ilike(f"%{filters['first_name']}%")
            )

        if "last_name" in filters:
            conditions.append(
                IDHWPerson.last_name.ilike(f"%{filters['last_name']}%")
            )

        if "gender" in filters:
            conditions.append(IDHWPerson.gender == filters["gender"])

        if "ssn" in filters:
            conditions.append(IDHWPerson.ssn == filters["ssn"])

        if "end_reason" in filters:
            conditions.append(IDHWPerson.end_reason == filters["end_reason"])

        # Date range filters
        if "dob" in filters:
            dob_filter = filters["dob"]
            if isinstance(dob_filter, tuple) and len(dob_filter) == 2:
                start, end = dob_filter
                conditions.append(
                    and_(
                        IDHWPerson.dob >= start,
                        IDHWPerson.dob <= end,
                    )
                )
            elif isinstance(dob_filter, datetime):
                conditions.append(IDHWPerson.dob == dob_filter)

        if "start_care_date" in filters:
            scd_filter = filters["start_care_date"]
            if isinstance(scd_filter, tuple) and len(scd_filter) == 2:
                start, end = scd_filter
                conditions.append(
                    and_(
                        IDHWPerson.start_care_date >= start,
                        IDHWPerson.start_care_date <= end,
                    )
                )
            elif isinstance(scd_filter, datetime):
                conditions.append(IDHWPerson.start_care_date == scd_filter)

        if "end_care_date" in filters:
            ecd_filter = filters["end_care_date"]
            if isinstance(ecd_filter, tuple) and len(ecd_filter) == 2:
                start, end = ecd_filter
                conditions.append(
                    and_(
                        IDHWPerson.end_care_date >= start,
                        IDHWPerson.end_care_date <= end,
                    )
                )
            elif isinstance(ecd_filter, datetime):
                conditions.append(IDHWPerson.end_care_date == ecd_filter)

        # Build query
        if conditions:
            stmt = select(IDHWPerson).where(and_(*conditions))
        else:
            stmt = select(IDHWPerson)

        result = await session.execute(stmt)
        rows = result.scalars().all()
        return [row.to_dict() for row in rows]


async def get_stats() -> dict[str, Any]:
    """Get overall statistics about IDHW records.

    Returns:
        Dictionary with counts and aggregations
    """
    session_maker = await get_pg_session("idhw")

    async with session_maker() as session:
        # Total records
        total_stmt = select(func.count(IDHWPerson.id))
        total = await session.execute(total_stmt)
        total_count = total.scalar() or 0

        # Children count
        children_stmt = select(func.count(IDHWPerson.id)).where(
            IDHWPerson.person_type == "child"
        )
        children = await session.execute(children_stmt)
        children_count = children.scalar() or 0

        # Foster children count
        foster_stmt = select(func.count(IDHWPerson.id)).where(
            and_(
                IDHWPerson.person_type == "child",
                IDHWPerson.start_care_date.isnot(None),
            )
        )
        foster = await session.execute(foster_stmt)
        foster_count = foster.scalar() or 0

        # TPR count
        tpr_stmt = select(func.count(IDHWPerson.id)).where(
            IDHWPerson.tpr_date.isnot(None)
        )
        tpr = await session.execute(tpr_stmt)
        tpr_count = tpr.scalar() or 0

        # Death count
        death_stmt = select(func.count(IDHWPerson.id)).where(
            IDHWPerson.death_date.isnot(None)
        )
        death = await session.execute(death_stmt)
        death_count = death.scalar() or 0

        return {
            "total_records": total_count,
            "children": children_count,
            "foster_children": foster_count,
            "terminations_of_parental_rights": tpr_count,
            "deaths": death_count,
        }
