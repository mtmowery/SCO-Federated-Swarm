"""
IDOCAgent: Multi-capability agent for Idaho Department of Corrections data.

Orchestrates queries against IDOC sentence and offender records with
support for person lookups, incarceration status checks, and aggregations.
"""

import logging
from uuid import UUID, uuid4
from datetime import datetime
from typing import Any, Optional

from src.shared.schemas import (
    AgentResponse,
    ResponseStatus,
    QueryType,
    AgencyName,
    Provenance,
)
from . import db

logger = logging.getLogger(__name__)

# IDOC domain system prompt
IDOC_SYSTEM_PROMPT = """You are an IDOC (Idaho Department of Corrections) domain expert assistant.

Your role:
- Answer questions about adult incarceration, sentencing, and offender records in Idaho
- Provide accurate crime statistics and offense aggregations
- Cross-reference offenders by insight_id, offender number, or personal identifiers
- Assess incarceration status for individuals and groups

Key data you manage:
- Sentence records: offender info, sentence dates, offense descriptions, status
- Offense types: crime groups, crime descriptions
- Status values: ACTIVE, DISCHARGED, PROBATION, etc.

When responding:
1. Clarify what data you're accessing (insight_id lookups, aggregate stats, etc)
2. Provide confidence in results based on data availability
3. Always note the source agency (IDOC) and data domain
4. Flag any missing or uncertain information

Security note: All responses are tagged CONFIDENTIAL per Idaho statutes."""


class IDOCAgent:
    """
    Multi-capability agent for IDOC data queries.

    Supports:
    - Individual person lookups by insight_id
    - Bulk incarceration status checks
    - Aggregate offense/status statistics
    - Natural language question answering
    """

    def __init__(self, agent_id: Optional[str] = None):
        """
        Initialize IDOC agent.

        Args:
            agent_id: Optional custom agent identifier
        """
        self.agent_id = agent_id or f"idoc-agent-{uuid4().hex[:8]}"
        self.agency = AgencyName.IDOC
        self.system_prompt = IDOC_SYSTEM_PROMPT

    async def query(self, question: str, request_id: Optional[UUID] = None) -> AgentResponse:
        """
        Answer a natural language question about IDOC data.

        This is a basic implementation that can be extended with LLM-based
        question classification and multi-tool orchestration.

        Args:
            question: Natural language question
            request_id: Optional request tracking ID

        Returns:
            AgentResponse with query results
        """
        request_id = request_id or uuid4()

        try:
            logger.info(f"Processing question: {question}")

            # Attempt to classify the question and route to appropriate handler
            question_lower = question.lower()

            # Route based on keywords
            if any(
                kw in question_lower
                for kw in ["how many", "count", "total", "statistics", "aggregate"]
            ):
                # Aggregate statistics query
                return await self._handle_aggregate_query(question, request_id)

            elif any(
                kw in question_lower
                for kw in ["offense", "crime", "charge", "by group", "by type"]
            ):
                # Offense aggregation
                return await self._handle_offense_summary(question, request_id)

            elif any(kw in question_lower for kw in ["incarcerate", "prison", "jail"]):
                # Incarceration status
                return await self._handle_incarceration_query(question, request_id)

            else:
                # Generic data lookup or status query
                return await self._handle_general_lookup(question, request_id)

        except Exception as e:
            logger.error(f"Query failed: {e}", exc_info=True)
            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.ERROR,
                data={"question": question},
                provenance=[
                    Provenance(
                        agency=self.agency,
                        query_type=QueryType.LOOKUP,
                    )
                ],
                confidence=0.0,
                security_tags=["CONFIDENTIAL"],
                error_message=f"Query processing failed: {str(e)}",
            )

    async def check_incarceration_status(
        self, insight_ids: list[str], request_id: Optional[UUID] = None
    ) -> AgentResponse:
        """
        Check incarceration status for a list of people.

        Args:
            insight_ids: List of cross-agency person identifiers
            request_id: Optional request tracking ID

        Returns:
            AgentResponse with incarceration status dict
        """
        request_id = request_id or uuid4()

        try:
            logger.info(f"Checking incarceration for {len(insight_ids)} people")

            status = await db.check_incarceration(insight_ids)

            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.SUCCESS,
                data={
                    "incarceration_status": status,
                    "query_count": len(insight_ids),
                    "incarcerated_count": sum(1 for v in status.values() if v),
                },
                provenance=[
                    Provenance(
                        agency=self.agency,
                        query_type=QueryType.LOOKUP,
                    )
                ],
                confidence=1.0,
                security_tags=["CONFIDENTIAL"],
            )

        except Exception as e:
            logger.error(f"Incarceration status check failed: {e}", exc_info=True)
            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.ERROR,
                data={"query_count": len(insight_ids)},
                provenance=[
                    Provenance(
                        agency=self.agency,
                        query_type=QueryType.LOOKUP,
                    )
                ],
                confidence=0.0,
                security_tags=["CONFIDENTIAL"],
                error_message=f"Incarceration check failed: {str(e)}",
            )

    async def count_incarcerated_parents(
        self, parent_ids: list[str], request_id: Optional[UUID] = None
    ) -> AgentResponse:
        """
        Count how many parents from a list have active sentences.

        Useful for child welfare cross-agency checks (IDHW linking).

        Args:
            parent_ids: List of cross-agency parent identifiers
            request_id: Optional request tracking ID

        Returns:
            AgentResponse with incarceration count
        """
        request_id = request_id or uuid4()

        try:
            logger.info(f"Counting incarcerated parents from {len(parent_ids)} identifiers")

            count = await db.count_incarcerated_from_ids(parent_ids)

            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.SUCCESS,
                data={
                    "query_count": len(parent_ids),
                    "incarcerated_count": count,
                    "percentage": round(100.0 * count / len(parent_ids), 2) if parent_ids else 0.0,
                },
                provenance=[
                    Provenance(
                        agency=self.agency,
                        query_type=QueryType.AGGREGATE,
                    )
                ],
                confidence=1.0,
                security_tags=["CONFIDENTIAL"],
            )

        except Exception as e:
            logger.error(f"Parent incarceration count failed: {e}", exc_info=True)
            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.ERROR,
                data={"query_count": len(parent_ids)},
                provenance=[
                    Provenance(
                        agency=self.agency,
                        query_type=QueryType.AGGREGATE,
                    )
                ],
                confidence=0.0,
                security_tags=["CONFIDENTIAL"],
                error_message=f"Parent incarceration count failed: {str(e)}",
            )

    async def aggregate_offenses(
        self, request_id: Optional[UUID] = None
    ) -> AgentResponse:
        """
        Get aggregate offense statistics by crime group.

        Returns:
            AgentResponse with crime group aggregations
        """
        request_id = request_id or uuid4()

        try:
            logger.info("Generating offense summary")

            summary = await db.get_offense_summary()
            status_counts = await db.count_by_status()

            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.SUCCESS,
                data={
                    "offense_summary": summary,
                    "status_summary": status_counts,
                    "total_sentences": sum(summary.values()),
                },
                provenance=[
                    Provenance(
                        agency=self.agency,
                        query_type=QueryType.AGGREGATE,
                    )
                ],
                confidence=1.0,
                security_tags=["CONFIDENTIAL"],
            )

        except Exception as e:
            logger.error(f"Offense aggregation failed: {e}", exc_info=True)
            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.ERROR,
                data={},
                provenance=[
                    Provenance(
                        agency=self.agency,
                        query_type=QueryType.AGGREGATE,
                    )
                ],
                confidence=0.0,
                security_tags=["CONFIDENTIAL"],
                error_message=f"Offense aggregation failed: {str(e)}",
            )

    # Private helper methods

    async def _handle_aggregate_query(
        self, question: str, request_id: UUID
    ) -> AgentResponse:
        """Handle aggregate statistics queries."""
        try:
            summary = await db.get_offense_summary()
            status_counts = await db.count_by_status()

            answer = (
                f"IDOC currently has {sum(summary.values())} total sentence records. "
                f"Sentence statuses: {status_counts}. "
                f"Offenses by group: {summary}"
            )

            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.SUCCESS,
                data={
                    "answer": answer,
                    "offense_summary": summary,
                    "status_summary": status_counts,
                },
                provenance=[
                    Provenance(
                        agency=self.agency,
                        query_type=QueryType.AGGREGATE,
                    )
                ],
                confidence=0.95,
                security_tags=["CONFIDENTIAL"],
            )
        except Exception as e:
            raise

    async def _handle_offense_summary(
        self, question: str, request_id: UUID
    ) -> AgentResponse:
        """Handle offense summary queries."""
        try:
            summary = await db.get_offense_summary()

            answer = f"IDOC offense statistics by crime group: {summary}"

            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.SUCCESS,
                data={"answer": answer, "offense_summary": summary},
                provenance=[
                    Provenance(
                        agency=self.agency,
                        query_type=QueryType.AGGREGATE,
                    )
                ],
                confidence=1.0,
                security_tags=["CONFIDENTIAL"],
            )
        except Exception as e:
            raise

    async def _handle_incarceration_query(
        self, question: str, request_id: UUID
    ) -> AgentResponse:
        """Handle incarceration-related queries."""
        try:
            # Get status counts
            status_counts = await db.count_by_status()

            active_count = status_counts.get("ACTIVE", 0)
            discharged_count = status_counts.get("DISCHARGED", 0)
            total = sum(status_counts.values())

            answer = (
                f"IDOC has {active_count} active sentences and {discharged_count} discharged. "
                f"Total records: {total}. Status breakdown: {status_counts}"
            )

            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.SUCCESS,
                data={"answer": answer, "status_summary": status_counts},
                provenance=[
                    Provenance(
                        agency=self.agency,
                        query_type=QueryType.LOOKUP,
                    )
                ],
                confidence=0.95,
                security_tags=["CONFIDENTIAL"],
            )
        except Exception as e:
            raise

    async def _handle_general_lookup(
        self, question: str, request_id: UUID
    ) -> AgentResponse:
        """Handle generic data lookup queries."""
        try:
            # Get sample records and basic stats
            sentences = await db.get_all_sentences(limit=5)
            summary = await db.get_offense_summary()

            answer = (
                f"I found {len(sentences)} recent IDOC sentence records. "
                f"The system tracks {len(summary)} different crime groups. "
                f"For specific lookups, I can search by name, offender number, or insight_id."
            )

            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.SUCCESS,
                data={
                    "answer": answer,
                    "sample_records": sentences,
                    "crime_groups": len(summary),
                },
                provenance=[
                    Provenance(
                        agency=self.agency,
                        query_type=QueryType.LOOKUP,
                    )
                ],
                confidence=0.85,
                security_tags=["CONFIDENTIAL"],
            )
        except Exception as e:
            raise
