"""
Audit Logging

Records all cross-agency queries, tool executions, and data access
for CJIS/HIPAA compliance. Every query is logged with:
- Who asked (requestor agent)
- What was asked (query text)
- Which agencies were consulted
- What was returned (aggregate summary, never raw PII)
- When it happened
- Confidence of the result
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from shared.config import get_settings

logger = logging.getLogger(__name__)


class AuditLogger:
    """
    Writes audit records to PostgreSQL and structured logs.

    All cross-agency operations are recorded for compliance review.
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._engine = None

    async def _get_engine(self):
        if self._engine is None:
            s = self.settings.postgres
            url = f"postgresql+asyncpg://{s.user}:{s.password}@{s.host}:{s.port}/{s.idhw_database}"
            self._engine = create_async_engine(url)
            await self._ensure_audit_table()
        return self._engine

    async def _ensure_audit_table(self) -> None:
        """Create audit table if it doesn't exist."""
        async with self._engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id              SERIAL PRIMARY KEY,
                    request_id      VARCHAR(50) NOT NULL,
                    requestor       VARCHAR(50),
                    action          VARCHAR(100),
                    agency          VARCHAR(20),
                    query_text      TEXT,
                    result_summary  TEXT,
                    result_count    INTEGER,
                    confidence      NUMERIC(4,3),
                    agencies_used   TEXT,
                    execution_time_ms INTEGER,
                    error           TEXT,
                    timestamp       TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_audit_request "
                "ON audit_log(request_id)"
            ))
            await conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_audit_timestamp "
                "ON audit_log(timestamp)"
            ))

    async def log_query(
        self,
        request_id: str,
        requestor: str,
        action: str,
        query_text: str,
        agency: str | None = None,
        result_summary: str | None = None,
        result_count: int | None = None,
        confidence: float | None = None,
        agencies_used: list[str] | None = None,
        execution_time_ms: int | None = None,
        error: str | None = None,
    ) -> None:
        """Log a query/action to the audit table."""
        engine = await self._get_engine()
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("""
                        INSERT INTO audit_log
                        (request_id, requestor, action, agency, query_text,
                         result_summary, result_count, confidence,
                         agencies_used, execution_time_ms, error)
                        VALUES
                        (:request_id, :requestor, :action, :agency, :query_text,
                         :result_summary, :result_count, :confidence,
                         :agencies_used, :execution_time_ms, :error)
                    """),
                    {
                        "request_id": request_id,
                        "requestor": requestor,
                        "action": action,
                        "agency": agency,
                        "query_text": query_text,
                        "result_summary": result_summary,
                        "result_count": result_count,
                        "confidence": confidence,
                        "agencies_used": ",".join(agencies_used) if agencies_used else None,
                        "execution_time_ms": execution_time_ms,
                        "error": error,
                    },
                )
            logger.info(
                f"Audit logged: {action} by {requestor} "
                f"(request={request_id}, agencies={agencies_used})"
            )
        except Exception as e:
            # Audit logging should never crash the main flow
            logger.error(f"Failed to write audit log: {e}")

    async def log_cross_agency_query(
        self,
        question: str,
        result: dict,
        agencies: list[str],
        execution_time_ms: int,
    ) -> str:
        """Convenience method for logging a full cross-agency query."""
        request_id = str(uuid.uuid4())[:12]
        await self.log_query(
            request_id=request_id,
            requestor="controller",
            action="cross_agency_query",
            query_text=question,
            result_summary=result.get("answer", str(result.get("count", ""))),
            result_count=result.get("count"),
            confidence=result.get("confidence"),
            agencies_used=agencies,
            execution_time_ms=execution_time_ms,
        )
        return request_id

    async def get_recent_queries(self, limit: int = 20) -> list[dict]:
        """Retrieve recent audit entries for review."""
        engine = await self._get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT * FROM audit_log "
                    "ORDER BY timestamp DESC LIMIT :limit"
                ),
                {"limit": limit},
            )
            rows = result.mappings().all()
            return [dict(row) for row in rows]

    async def close(self) -> None:
        if self._engine:
            await self._engine.dispose()
            self._engine = None


class PolicyEngine:
    """
    Simple policy engine for query governance.

    Phase 4+ would use OPA or Cedar. For MVP, this implements
    basic aggregate-only and PII masking rules.
    """

    # Queries that are always denied
    DENIED_PATTERNS = [
        "list all names",
        "show ssn",
        "show social security",
        "export all records",
        "dump database",
    ]

    # Operations that require aggregate-only output
    AGGREGATE_REQUIRED = [
        "count",
        "how many",
        "total",
        "statistics",
        "summary",
    ]

    def evaluate_query(self, question: str) -> dict[str, Any]:
        """
        Evaluate whether a query is permitted.

        Returns:
            {"allowed": True/False, "reason": str, "policy": str}
        """
        q_lower = question.lower()

        # Check denied patterns
        for pattern in self.DENIED_PATTERNS:
            if pattern in q_lower:
                return {
                    "allowed": False,
                    "reason": f"Query matches denied pattern: '{pattern}'",
                    "policy": "pii_protection",
                }

        # Check if aggregate output required
        requires_aggregate = any(p in q_lower for p in self.AGGREGATE_REQUIRED)

        return {
            "allowed": True,
            "reason": "Query permitted",
            "policy": "aggregate_only" if requires_aggregate else "standard",
            "aggregate_required": requires_aggregate,
        }

    def mask_pii(self, record: dict) -> dict:
        """Mask PII fields in a record for safe output."""
        pii_fields = {"ssn", "ssn_nbr", "SSN", "social_security"}
        masked = {}
        for key, value in record.items():
            if key.lower() in {f.lower() for f in pii_fields}:
                masked[key] = "***MASKED***"
            else:
                masked[key] = value
        return masked
