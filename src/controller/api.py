"""
Controller REST API

Exposes the federated swarm through a FastAPI application.
Endpoints:
  POST /query            — Run a natural language query through the swarm
  GET  /health           — Service health
  GET  /audit/recent     — Recent audit entries
"""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from controller.graph import build_graph, run_query
from security.audit import AuditLogger, PolicyEngine
from shared.config import settings

logger = logging.getLogger(__name__)


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=2000)
    requestor: str = Field(default="api_user")


class QueryResponse(BaseModel):
    answer: str
    confidence: float
    sources: list[str]
    intent: str | None = None
    errors: list[str] = []
    request_id: str | None = None
    execution_time_ms: int | None = None


def create_app() -> FastAPI:
    """Factory for the controller FastAPI application."""

    app = FastAPI(
        title="Idaho Federated AI Swarm",
        description="Cross-agency intelligence mesh controller",
        version=settings.api_version,
    )

    audit = AuditLogger()
    policy = PolicyEngine()

    @app.get("/health")
    async def health():
        return {"status": "healthy", "service": "controller"}

    @app.post("/query", response_model=QueryResponse)
    async def query_endpoint(req: QueryRequest) -> QueryResponse:
        # Policy check
        policy_result = policy.evaluate_query(req.question)
        if not policy_result["allowed"]:
            raise HTTPException(
                status_code=403,
                detail=policy_result["reason"],
            )

        start = time.monotonic()
        result = await run_query(req.question)
        elapsed_ms = int((time.monotonic() - start) * 1000)

        request_id = await audit.log_cross_agency_query(
            question=req.question,
            result=result,
            agencies=result.get("sources", []),
            execution_time_ms=elapsed_ms,
        )

        return QueryResponse(
            answer=result.get("answer", ""),
            confidence=result.get("confidence", 0.0),
            sources=result.get("sources", []),
            intent=result.get("intent"),
            errors=result.get("errors", []),
            request_id=request_id,
            execution_time_ms=elapsed_ms,
        )

    @app.get("/audit/recent")
    async def recent_audit(limit: int = 20):
        entries = await audit.get_recent_queries(limit=limit)
        return {"entries": entries, "count": len(entries)}

    @app.on_event("shutdown")
    async def shutdown():
        await audit.close()

    return app
