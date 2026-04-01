"""SQLAlchemy ORM models for IDHW foster care data.

Maps CSV schema to database tables with proper indexing and constraints.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    String,
    Integer,
    DateTime,
    Boolean,
    Float,
    Index,
    ForeignKey,
)
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""

    pass


class IDHWPerson(Base):
    """IDHW person record mapping foster care data.

    Represents individuals in the foster care system (children, mothers, fathers).
    """

    __tablename__ = "idhw_persons"

    insight_id: Mapped[str] = mapped_column(
        String(255), primary_key=True, index=True
    )

    # Family relationship insight_ids (foreign keys to other records)
    child_insight_id: Mapped[Optional[str]] = mapped_column(
        String(255), index=True, nullable=True
    )
    mother_insight_id: Mapped[Optional[str]] = mapped_column(
        String(255), index=True, nullable=True
    )
    father_insight_id: Mapped[Optional[str]] = mapped_column(
        String(255), index=True, nullable=True
    )

    # Extracted date components (for analysis)
    dob_month: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    dob_year: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)

    # Person type (child, mother, father)
    person_type: Mapped[str] = mapped_column(
        String(50), nullable=False, index=True
    )

    # Personal information
    gender: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Foster care dates and status
    start_care_date: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True, index=True
    )
    end_care_date: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True, index=True
    )
    end_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Legal events
    tpr_date: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True
    )  # Termination of Parental Rights
    death_date: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    deceased_before_removal: Mapped[Optional[str]] = mapped_column(
        "deceasead_before_removal", String(10), nullable=True
    )

    # Incarceration status
    incarcerated_at_removal: Mapped[Optional[str]] = mapped_column(
        String(10), nullable=True
    )
    father_not_in_home: Mapped[Optional[str]] = mapped_column(
        String(10), nullable=True
    )

    # Data quality
    errors: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)

    # Composite indexes for common queries
    __table_args__ = (
        Index(
            "idx_idhw_child_parents",
            "child_insight_id",
            "mother_insight_id",
            "father_insight_id",
        ),
        Index("idx_idhw_foster_care_dates", "start_care_date", "end_care_date"),
        Index("idx_idhw_person_lookup", "person_type"),
    )

    def to_dict(self) -> dict:
        return {
            "insight_id": self.insight_id,
            "child_insight_id": self.child_insight_id,
            "mother_insight_id": self.mother_insight_id,
            "father_insight_id": self.father_insight_id,
            "dob_month": self.dob_month,
            "dob_year": self.dob_year,
            "person_type": self.person_type,
            "gender": self.gender,
            "start_care_date": self.start_care_date,
            "end_care_date": self.end_care_date,
            "end_reason": self.end_reason,
            "tpr_date": self.tpr_date,
            "death_date": self.death_date,
            "deceased_before_removal": self.deceased_before_removal,
            "incarcerated_at_removal": self.incarcerated_at_removal,
            "father_not_in_home": self.father_not_in_home,
            "errors": self.errors,
        }
