"""SQLAlchemy ORM models for IDJC (Idaho Department of Juvenile Corrections).

Defines the IDJCCommitment model mapping all CSV columns with proper types,
indexes, and SQLAlchemy 2.0 declarative style.
"""

from datetime import datetime, date
from sqlalchemy import (
    Column,
    String,
    Date,
    DateTime,
    Integer,
    Index,
    Text,
)
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from typing import Optional

Base = declarative_base()


class IDJCCommitment(Base):
    """
    SQLAlchemy ORM model for IDJC commitment records.

    Maps all columns from the IDJC CSV schema with proper types.
    Includes indexes on key columns for efficient queries.
    """

    __tablename__ = "idjc_commitments"

    # Primary key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Identity columns
    insight_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    ijos_id: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)

    # Personal information
    last_name: Mapped[str] = mapped_column(String(100), nullable=False)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    middle_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Demographic data
    dob: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    gender: Mapped[Optional[str]] = mapped_column(String(1), nullable=True)
    ssn: Mapped[Optional[str]] = mapped_column(String(11), nullable=True)

    # Commitment information
    date_of_commitment: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    date_of_release: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    committing_county: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Offense information
    offense_number: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    statute_code: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    offense_description: Mapped[Optional[Text]] = mapped_column(Text, nullable=True)
    offense_category: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)
    offense_level: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)
    significance_level: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Status tracking
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True, default="Active")

    # Audit columns
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    def __repr__(self) -> str:
        return (
            f"<IDJCCommitment("
            f"insight_id={self.insight_id}, "
            f"ijos_id={self.ijos_id}, "
            f"name={self.first_name} {self.last_name}, "
            f"status={self.status}"
            f")>"
        )

    def to_dict(self) -> dict:
        """Convert model instance to dictionary."""
        return {
            "id": self.id,
            "insight_id": self.insight_id,
            "ijos_id": self.ijos_id,
            "last_name": self.last_name,
            "first_name": self.first_name,
            "middle_name": self.middle_name,
            "dob": self.dob.isoformat() if self.dob else None,
            "gender": self.gender,
            "ssn": self.ssn,
            "date_of_commitment": self.date_of_commitment.isoformat()
            if self.date_of_commitment
            else None,
            "date_of_release": self.date_of_release.isoformat()
            if self.date_of_release
            else None,
            "committing_county": self.committing_county,
            "offense_number": self.offense_number,
            "statute_code": self.statute_code,
            "offense_description": self.offense_description,
            "offense_category": self.offense_category,
            "offense_level": self.offense_level,
            "significance_level": self.significance_level,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


# Define composite indexes for common query patterns
__table_args__ = (
    Index("idx_insight_id_status", "insight_id", "status"),
    Index("idx_insight_id_offense_category", "insight_id", "offense_category"),
    Index("idx_commitment_dates", "date_of_commitment", "date_of_release"),
    Index("idx_county_status", "committing_county", "status"),
)

IDJCCommitment.__table_args__ = __table_args__
