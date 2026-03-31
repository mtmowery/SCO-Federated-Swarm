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

    # Agency identifiers
    agency_id: Mapped[str] = mapped_column(String(50), nullable=False)
    child_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    mother_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    father_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Person type (child, mother, father)
    person_type: Mapped[str] = mapped_column(
        String(50), nullable=False, index=True
    )

    # Personal information
    first_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    middle_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    dob: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    ssn: Mapped[Optional[str]] = mapped_column(
        String(11), nullable=True
    )  # Format: XXX-XX-XXXX
    gender: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Foster care dates and status
    start_care_date: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, index=True
    )
    end_care_date: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, index=True
    )
    end_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Legal events
    tpr_date: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )  # Termination of Parental Rights
    death_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    deceased_before_removal: Mapped[Optional[bool]] = mapped_column(
        "deceasead_before_removal", Boolean, nullable=True
    )

    # Incarceration status
    incarcerated_at_removal: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    father_not_in_home: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )

    # Extracted date components (for analysis)
    dob_month: Mapped[Optional[Integer]] = mapped_column(Integer, nullable=True)
    dob_year: Mapped[Optional[Integer]] = mapped_column(Integer, nullable=True)

    # Data quality
    errors: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)

    # No audit timestamps in CSV

    # Composite indexes for common queries
    __table_args__ = (
        Index(
            "idx_idhw_child_parents",
            "child_insight_id",
            "mother_insight_id",
            "father_insight_id",
        ),
        Index("idx_idhw_foster_care_dates", "start_care_date", "end_care_date"),
        Index("idx_idhw_person_lookup", "person_type", "agency_id"),
    )

    def to_dict(self) -> dict:
        return {
            "insight_id": self.insight_id,
            "child_insight_id": self.child_insight_id,
            "mother_insight_id": self.mother_insight_id,
            "father_insight_id": self.father_insight_id,
            "agency_id": self.agency_id,
            "child_id": self.child_id,
            "mother_id": self.mother_id,
            "father_id": self.father_id,
            "person_type": self.person_type,
            "first_name": self.first_name,
            "middle_name": self.middle_name,
            "last_name": self.last_name,
            "dob": self.dob.isoformat() if hasattr(self.dob, 'isoformat') else self.dob,
            "ssn": self.ssn,
            "gender": self.gender,
            "start_care_date": self.start_care_date.isoformat() if hasattr(self.start_care_date, 'isoformat') else self.start_care_date,
            "end_care_date": self.end_care_date.isoformat() if hasattr(self.end_care_date, 'isoformat') else self.end_care_date,
            "end_reason": self.end_reason,
            "tpr_date": self.tpr_date.isoformat() if hasattr(self.tpr_date, 'isoformat') else self.tpr_date,
            "death_date": self.death_date.isoformat() if hasattr(self.death_date, 'isoformat') else self.death_date,
            "deceased_before_removal": self.deceased_before_removal,
            "incarcerated_at_removal": self.incarcerated_at_removal,
            "father_not_in_home": self.father_not_in_home,
            "dob_month": self.dob_month,
            "dob_year": self.dob_year,
            "errors": self.errors,
        }
