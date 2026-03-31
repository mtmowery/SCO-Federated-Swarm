"""IDJC Agent for handling juvenile commitment queries and cross-agency lookups.

Provides domain-specific agent interface with LLM integration for natural language
query understanding and response synthesis.
"""

import logging
from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from shared.config import settings
from shared.schemas import (
    AgentQuery,
    AgentResponse,
    Provenance,
    ResponseStatus,
    QueryType,
    AgencyName,
)
from . import db

logger = logging.getLogger(__name__)


# IDJC Domain-specific system prompt
IDJC_SYSTEM_PROMPT = """You are an expert agent for the Idaho Department of Juvenile Corrections (IDJC).
Your domain covers:
- Juvenile commitment and detention records
- Offense information and categorization
- Commitment status tracking (Active, Released, etc.)
- Cross-agency identity linkage via insight_id
- Demographic and offense pattern analysis

When answering questions:
1. Use accurate terminology specific to IDJC (IJOS_ID, commitment dates, offense levels, etc.)
2. Consider privacy implications of juvenile records
3. Provide context about offense categories and significance levels
4. Highlight important status changes or patterns
5. When cross-agency information is needed, indicate which agencies should be consulted
6. Always cite the confidence level of your analysis

Available data sources:
- Commitment records with complete offense histories
- Status tracking (Active, Released, etc.)
- County-level commitment data
- Offense summaries and aggregations
- Bulk identity checking for juvenile record presence

Always maintain confidentiality standards for juvenile information."""


class IDJCAgent:
    """Agent for IDJC (Idaho Department of Juvenile Corrections).

    Handles juvenile commitment queries, offense analysis, and cross-agency
    identity verification through insight_id linkage.
    """

    def __init__(self):
        """Initialize IDJC agent."""
        self.agency = AgencyName.IDJC
        self.agent_id = "idjc-agent-v1"
        self.system_prompt = IDJC_SYSTEM_PROMPT

    async def query(
        self, question: str, filters: Optional[dict[str, Any]] = None
    ) -> AgentResponse:
        """
        Process a natural language query about IDJC commitments.

        Args:
            question: Natural language question
            filters: Optional query filters

        Returns:
            AgentResponse with query results and provenance
        """
        request_id = uuid4()
        filters = filters or {}

        try:
            logger.info(f"Processing IDJC query: {question}")

            # Determine query type based on question content
            query_type = self._classify_query(question)

            # Route to appropriate handler
            if "active" in question.lower() and "commitment" in question.lower():
                result = await self._handle_active_commitments(question)
            elif "offense" in question.lower() and ("summary" in question.lower() or "aggregate" in question.lower() or "count" in question.lower()):
                result = await self._handle_offense_summary(question)
            elif "status" in question.lower() and ("count" in question.lower() or "summary" in question.lower()):
                result = await self._handle_status_summary(question)
            elif "search" in question.lower() or "find" in question.lower():
                result = await self._handle_search(question, filters)
            else:
                result = await self._handle_general_query(question, filters)

            # Build response
            provenance = Provenance(
                agency=self.agency,
                query_type=query_type,
                timestamp=datetime.utcnow(),
                schema_version="1.0",
            )

            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.SUCCESS,
                data=result,
                provenance=[provenance],
                confidence=0.9,
            )

        except Exception as e:
            logger.error(f"Error processing IDJC query: {e}")
            provenance = Provenance(
                agency=self.agency,
                query_type=QueryType.LOOKUP,
                timestamp=datetime.utcnow(),
                schema_version="1.0",
            )

            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.ERROR,
                error_message=str(e),
                provenance=[provenance],
                confidence=0.0,
            )

    async def check_juvenile_history(self, insight_ids: list[str]) -> dict[str, Any]:
        """
        Check which individuals have juvenile records in IDJC.

        Args:
            insight_ids: List of insight identifiers to check

        Returns:
            Dictionary with results and summary statistics
        """
        try:
            logger.info(f"Checking juvenile history for {len(insight_ids)} individuals")

            results = await db.check_juvenile_record(insight_ids)
            with_records = sum(1 for v in results.values() if v)

            return {
                "operation": "juvenile_history_check",
                "total_checked": len(insight_ids),
                "with_records": with_records,
                "without_records": len(insight_ids) - with_records,
                "results": results,
            }

        except Exception as e:
            logger.error(f"Error checking juvenile history: {e}")
            return {"error": str(e)}

    async def aggregate_offenses(self) -> dict[str, Any]:
        """
        Get offense summary and distribution analysis.

        Returns:
            Dictionary with offense aggregations and statistics
        """
        try:
            logger.info("Aggregating offense data")

            summary = await db.get_offense_summary()
            status_counts = await db.count_by_status()

            total_commitments = sum(summary.values())
            top_offenses = sorted(
                summary.items(), key=lambda x: x[1], reverse=True
            )[:10]

            return {
                "operation": "offense_aggregation",
                "total_commitments": total_commitments,
                "offense_categories": len(summary),
                "top_10_offenses": [
                    {"category": cat, "count": count} for cat, count in top_offenses
                ],
                "status_distribution": status_counts,
                "offense_summary": summary,
            }

        except Exception as e:
            logger.error(f"Error aggregating offenses: {e}")
            return {"error": str(e)}

    async def get_active_youth(self) -> dict[str, Any]:
        """
        Get statistics about currently active youth in IDJC.

        Returns:
            Dictionary with active youth statistics
        """
        try:
            logger.info("Retrieving active youth statistics")

            active = await db.get_active_commitments(limit=10000)
            status_counts = await db.count_by_status()

            active_count = status_counts.get("Active", 0)
            released_count = status_counts.get("Released", 0)

            return {
                "operation": "active_youth_summary",
                "currently_active": active_count,
                "total_released": released_count,
                "active_sample_count": len(active),
                "active_commitments_sample": active[:100],  # First 100 for display
            }

        except Exception as e:
            logger.error(f"Error retrieving active youth: {e}")
            return {"error": str(e)}

    def _classify_query(self, question: str) -> QueryType:
        """Classify query type based on question content."""
        question_lower = question.lower()

        if any(word in question_lower for word in ["count", "how many", "aggregate", "summary", "distribution"]):
            return QueryType.AGGREGATE
        elif any(word in question_lower for word in ["look up", "find", "get", "search"]):
            return QueryType.LOOKUP
        elif any(word in question_lower for word in ["relationship", "related", "connection", "linked"]):
            return QueryType.RELATIONSHIP
        else:
            return QueryType.LOOKUP

    async def _handle_active_commitments(self, question: str) -> dict[str, Any]:
        """Handle queries about active commitments."""
        active = await db.get_active_commitments(limit=1000)
        return {
            "query_type": "active_commitments",
            "results": active,
            "count": len(active),
            "explanation": f"Retrieved {len(active)} active commitments from IDJC",
        }

    async def _handle_offense_summary(self, question: str) -> dict[str, Any]:
        """Handle queries about offense summaries."""
        summary = await db.get_offense_summary()
        status_counts = await db.count_by_status()

        total = sum(summary.values())
        top_10 = sorted(summary.items(), key=lambda x: x[1], reverse=True)[:10]

        return {
            "query_type": "offense_summary",
            "total_commitments": total,
            "offense_categories": len(summary),
            "top_10_offenses": [
                {"category": cat, "count": count} for cat, count in top_10
            ],
            "full_summary": summary,
            "status_distribution": status_counts,
        }

    async def _handle_status_summary(self, question: str) -> dict[str, Any]:
        """Handle queries about commitment status."""
        status_counts = await db.count_by_status()
        total = sum(status_counts.values())

        return {
            "query_type": "status_summary",
            "status_distribution": status_counts,
            "total_commitments": total,
            "explanation": f"Total commitments: {total}, distributed across {len(status_counts)} status categories",
        }

    async def _handle_search(
        self, question: str, filters: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle search queries with filters."""
        results = await db.search_commitments(filters)

        return {
            "query_type": "search",
            "results": results,
            "count": len(results),
            "filters_applied": filters,
        }

    async def _handle_general_query(
        self, question: str, filters: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle general queries."""
        # Try to extract key terms and perform relevant queries
        query_lower = question.lower()

        if "insight_id" in question or "person" in query_lower:
            # Person lookup - would need insight_id from filters
            if "insight_id" in filters:
                results = await db.get_person_by_insight_id(filters["insight_id"])
                return {
                    "query_type": "person_lookup",
                    "results": results,
                    "count": len(results),
                }

        # Default: return basic statistics
        all_commitments = await db.get_all_commitments(limit=100)
        status_counts = await db.count_by_status()

        return {
            "query_type": "general",
            "summary": {
                "status_counts": status_counts,
                "sample_records": all_commitments,
            },
            "explanation": "Returning general IDJC statistics and sample records",
        }
