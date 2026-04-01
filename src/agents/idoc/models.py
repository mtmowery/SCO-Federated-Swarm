"""
SQLAlchemy ORM models for IDOC (Idaho Department of Corrections) sentencing data.

Models map to the IDOC CSV schema:
- insight_id, ofndr_num, fnam, lnam, mnam, dob_dtd, sex_cd, ssn_nbr
- incrno, mitt_srl, caseno, caseno_seq, state, cnty_sdesc
- sent_beg_dtd, sent_eff_dtd, sent_ft_dtd, consec_typ
- off_ldesc, crm_grp_desc, mitt_status, sent_status
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column,
    String,
    Date,
    DateTime,
    Integer,
    Index,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all IDOC ORM models."""

    pass


class IDOCSentence(Base):
    """
    IDOC sentence and offender records.

    Represents adult incarceration and sentencing information with
    indexes on common lookup fields.
    """

    __tablename__ = "idoc_sentences"

    # Primary identifier
    id: Mapped[int] = mapped_column(primary_key=True, index=True)

    # Cross-agency linkage
    insight_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    # Demographic data
    dob_month: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    dob_year: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    gender: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Geographic info
    state: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    cnty_sdesc: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Sentence dates
    sent_beg_dtd: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    sent_eff_dtd: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    sent_ft_dtd: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Sentence structure
    consec_typ: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Offense information
    off_ldesc: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    crm_grp_desc: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Status fields
    mitt_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)
    sent_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)

    def __repr__(self) -> str:
        """Return string representation of sentence record."""
        return (
            f"IDOCSentence(insight_id={self.insight_id}, "
            f"sent_status={self.sent_status})"
        )

    def to_dict(self) -> dict:
        """Convert record to dictionary."""
        return {
            "id": self.id,
            "insight_id": self.insight_id,
            "dob_month": self.dob_month,
            "dob_year": self.dob_year,
            "gender": self.gender,
            "state": self.state,
            "cnty_sdesc": self.cnty_sdesc,
            "sent_beg_dtd": self.sent_beg_dtd,
            "sent_eff_dtd": self.sent_eff_dtd,
            "sent_ft_dtd": self.sent_ft_dtd,
            "consec_typ": self.consec_typ,
            "off_ldesc": self.off_ldesc,
            "crm_grp_desc": self.crm_grp_desc,
            "mitt_status": self.mitt_status,
            "sent_status": self.sent_status,
        }

# Create indexes for optimal query performance
__table_args__ = (
    Index("idx_idoc_insight_id", IDOCSentence.insight_id),
    Index("idx_idoc_sent_status", IDOCSentence.sent_status),
    Index("idx_idoc_mitt_status", IDOCSentence.mitt_status),
    Index("idx_idoc_crm_grp_desc", IDOCSentence.crm_grp_desc),
)
