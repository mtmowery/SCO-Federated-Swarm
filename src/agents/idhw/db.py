"""Database query functions for IDHW foster care data.

Provides async database operations for person lookups, family relationships,
and aggregations.
"""

import logging
from typing import Optional, Any
from datetime import datetime

from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database import get_pg_session
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

    Includes care metadata so CrossAgencyReasoner can distinguish foster
    children (start_care_date is not null) from other IDHW child records
    and so answer synthesis can include care period context.

    Returns:
        List of dicts with keys:
          child_insight_id, mother_insight_id, father_insight_id,
          gender, dob_month, dob_year, start_care_date, end_care_date, end_reason
    """
    session_maker = await get_pg_session("idhw")

    async with session_maker() as session:
        stmt = select(
            IDHWPerson.child_insight_id,
            IDHWPerson.mother_insight_id,
            IDHWPerson.father_insight_id,
            IDHWPerson.insight_id,        # child's own insight_id (may equal child_insight_id)
            IDHWPerson.gender,
            IDHWPerson.dob_month,
            IDHWPerson.dob_year,
            IDHWPerson.start_care_date,
            IDHWPerson.end_care_date,
            IDHWPerson.end_reason,
        ).where(IDHWPerson.person_type == "child")

        result = await session.execute(stmt)
        rows = result.all()
        return [
            {
                "child_insight_id": row[0] or row[3],  # prefer explicit child_insight_id
                "mother_insight_id": row[1],
                "father_insight_id": row[2],
                "insight_id": row[3],
                "gender": row[4],
                "dob_month": str(row[5]) if row[5] else None,
                "dob_year": str(row[6]) if row[6] else None,
                "start_care_date": str(row[7]) if row[7] else None,
                "end_care_date": str(row[8]) if row[8] else None,
                "end_reason": row[9],
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
            select(IDHWPerson.end_reason, func.count(IDHWPerson.insight_id))
            .where(IDHWPerson.person_type == "child")
            .group_by(IDHWPerson.end_reason)
        )
        result = await session.execute(stmt)
        rows = result.all()
        return {str(reason): count for reason, count in rows}


async def search_people(filters: dict[str, Any]) -> list[dict[str, Any]]:
    """Flexible search with filters on any column.

    Supports filtering on:
    - insight_id
    - child_insight_id
    - mother_insight_id
    - father_insight_id
    - person_type: str
    - gender: str
    - end_reason: str
    - start_care_date: str (exact match)
    - end_care_date: str (exact match)

    Args:
        filters: Dictionary of filter criteria

    Returns:
        List of matching person records as dictionaries
    """
    session_maker = await get_pg_session("idhw")

    async with session_maker() as session:
        conditions = []

        # Build dynamic filters
        if "insight_id" in filters:
            conditions.append(IDHWPerson.insight_id == filters["insight_id"])
            
        if "child_insight_id" in filters:
            conditions.append(IDHWPerson.child_insight_id == filters["child_insight_id"])
            
        if "mother_insight_id" in filters:
            conditions.append(IDHWPerson.mother_insight_id == filters["mother_insight_id"])
            
        if "father_insight_id" in filters:
            conditions.append(IDHWPerson.father_insight_id == filters["father_insight_id"])

        if "person_type" in filters:
            conditions.append(IDHWPerson.person_type == filters["person_type"])

        if "gender" in filters:
            conditions.append(IDHWPerson.gender == filters["gender"])

        if "end_reason" in filters:
            conditions.append(IDHWPerson.end_reason == filters["end_reason"])

        if "start_care_date" in filters:
            conditions.append(IDHWPerson.start_care_date == filters["start_care_date"])

        if "end_care_date" in filters:
            conditions.append(IDHWPerson.end_care_date == filters["end_care_date"])

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
        total_stmt = select(func.count(IDHWPerson.insight_id))
        total = await session.execute(total_stmt)
        total_count = total.scalar() or 0

        # Children count
        children_stmt = select(func.count(IDHWPerson.insight_id)).where(
            IDHWPerson.person_type == "child"
        )
        children = await session.execute(children_stmt)
        children_count = children.scalar() or 0

        # Foster children count
        foster_stmt = select(func.count(IDHWPerson.insight_id)).where(
            and_(
                IDHWPerson.person_type == "child",
                IDHWPerson.start_care_date.isnot(None),
            )
        )
        foster = await session.execute(foster_stmt)
        foster_count = foster.scalar() or 0

        # TPR count
        tpr_stmt = select(func.count(IDHWPerson.insight_id)).where(
            IDHWPerson.tpr_date.isnot(None)
        )
        tpr = await session.execute(tpr_stmt)
        tpr_count = tpr.scalar() or 0

        # Death count
        death_stmt = select(func.count(IDHWPerson.insight_id)).where(
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
