"""FastAPI MCP server for IDJC (Idaho Department of Juvenile Corrections).

Provides HTTP endpoints for MCP tool execution, health checks, and capabilities.
"""

import logging
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Any, Optional

from shared.config import settings
from shared.database import DatabaseManager
from shared.schemas import AgencyCapability, AgencyName, ResponseStatus
from .tools import mcp

logger = logging.getLogger(__name__)


class ExecuteRequest(BaseModel):
    """MCP tool execution request."""

    tool_name: str = Field(description="Name of the tool to execute")
    arguments: dict[str, Any] = Field(
        default_factory=dict, description="Tool arguments"
    )


class ExecuteResponse(BaseModel):
    """MCP tool execution response."""

    status: str = Field(description="Status: success or error")
    result: Any = Field(default=None, description="Tool result or error message")


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(description="Service status")
    timestamp: str = Field(description="Check timestamp")
    database: str = Field(description="Database connection status")
    version: str = Field(description="Service version")


class CapabilitiesResponse(BaseModel):
    """Agency capabilities response."""

    agency: AgencyCapability = Field(description="Agency capability descriptor")
    tools: list[dict[str, Any]] = Field(description="Available tools")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    logger.info("IDJC MCP server starting...")
    yield
    logger.info("IDJC MCP server shutting down...")
    await DatabaseManager.close_all()


# Create FastAPI application
app = FastAPI(
    title="IDJC MCP Server",
    description="Model Context Protocol server for Idaho Department of Juvenile Corrections",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Health check endpoint."""
    try:
        # Test database connection
        engine = await DatabaseManager.get_pg_engine("idjc")
        async with engine.begin() as conn:
            await conn.exec_driver_sql("SELECT 1")
        db_status = "healthy"
    except Exception as e:
        logger.error(f"Health check database error: {e}")
        db_status = "unhealthy"

    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat(),
        database=db_status,
        version="1.0.0",
    )


@app.get("/capabilities", response_model=CapabilitiesResponse)
async def get_capabilities() -> CapabilitiesResponse:
    """Get agency capabilities and available tools."""
    capability = AgencyCapability(
        agent_id="idjc-agent-v1",
        agency=AgencyName.IDJC,
        version="1.0.0",
        description="Idaho Department of Juvenile Corrections commitment record agent",
        data_domain=["juvenile_commitments", "detention_records", "offense_data"],
        entities=["juveniles", "commitments", "offenses"],
        join_keys=["insight_id", "ssn", "ijos_id"],
        capabilities=["lookup", "aggregate", "bulk_check", "search", "filter"],
        security_level="confidential",
        last_updated=datetime.utcnow(),
    )

    tools = [
        {
            "name": "get_commitments",
            "description": "Get all IDJC commitments with pagination",
            "parameters": {
                "limit": "int (default: 1000)",
                "offset": "int (default: 0)",
            },
        },
        {
            "name": "get_person",
            "description": "Get all commitment records for a person by insight_id",
            "parameters": {"insight_id": "str (required)"},
        },
        {
            "name": "get_people_bulk",
            "description": "Bulk lookup: get records for multiple insight_ids",
            "parameters": {"insight_ids": "list[str] (required)"},
        },
        {
            "name": "get_active_commitments",
            "description": "Get all active commitments",
            "parameters": {
                "limit": "int (default: 1000)",
                "offset": "int (default: 0)",
            },
        },
        {
            "name": "check_juvenile_record",
            "description": "Check which insight_ids have juvenile records",
            "parameters": {"insight_ids": "list[str] (required)"},
        },
        {
            "name": "get_top_offenders",
            "description": "Get top 10 individuals with most offenses",
            "parameters": {"limit": "int (default: 10)"},
        },
        {
            "name": "get_offense_summary",
            "description": "Get aggregate counts by offense category",
            "parameters": {},
        },
        {
            "name": "count_by_status",
            "description": "Get commitment counts grouped by status",
            "parameters": {},
        },
        {
            "name": "search_commitments",
            "description": "Search commitments with flexible filtering",
            "parameters": {
                "insight_id": "str (optional)",
                "ijos_id": "str (optional)",
                "first_name": "str (optional)",
                "last_name": "str (optional)",
                "ssn": "str (optional)",
                "status": "str (optional)",
                "offense_category": "str (optional)",
                "offense_level": "str (optional)",
                "committing_county": "str (optional)",
                "significance_level": "str (optional)",
                "dob_start": "str ISO date (optional)",
                "dob_end": "str ISO date (optional)",
                "commitment_start": "str ISO date (optional)",
                "commitment_end": "str ISO date (optional)",
                "limit": "int (default: 1000)",
                "offset": "int (default: 0)",
            },
        },
    ]

    return CapabilitiesResponse(agency=capability, tools=tools)


@app.post("/execute", response_model=ExecuteResponse)
async def execute_tool(request: ExecuteRequest) -> ExecuteResponse:
    """Execute an MCP tool."""
    try:
        tool_name = request.tool_name
        arguments = request.arguments or {}

        logger.info(f"Executing tool: {tool_name} with args: {arguments}")

        from . import tools as idjc_tools

        # Use tools.py wrappers (not raw db functions) so return types
        # match the dict schemas the executor/controller expects.
        # Raw db functions return bare ints / flat dicts, while the
        # tools wrappers return structured dicts with named keys like
        # {"total_people_count": N}, {"counts": {...}, "total_records": N}.
        direct_tools = {
            "get_commitments": idjc_tools.get_commitments,
            "get_person": idjc_tools.get_person,
            "get_people_bulk": idjc_tools.get_people_bulk,
            "get_active_commitments": idjc_tools.get_active_commitments,
            "check_juvenile_record": idjc_tools.check_juvenile_record,
            "get_offense_summary": idjc_tools.get_offense_summary,
            "get_offense_breakdown": idjc_tools.get_offense_breakdown,
            "count_by_status": idjc_tools.count_by_status,
            "search_commitments": idjc_tools.search_commitments,
            "count_total_people": idjc_tools.count_total_people,
            "get_all_insight_ids": idjc_tools.get_all_insight_ids,
            "get_top_offenders": idjc_tools.get_top_offenders,
        }

        if tool_name not in direct_tools:
            raise HTTPException(
                status_code=404, detail=f"Tool not found: {tool_name}"
            )

        tool_func = direct_tools[tool_name]

        # Execute tool with arguments
        result = await tool_func(**arguments)

        logger.info(f"Tool {tool_name} executed successfully")

        return ExecuteResponse(status="success", result=result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Tool execution error: {e}")
        return ExecuteResponse(
            status="error", result={"error": str(e), "tool": request.tool_name}
        )


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Add request ID to all responses for tracing."""
    request_id = request.headers.get("X-Request-ID", "no-id")
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


@app.get("/")
async def root():
    """Root endpoint with service info."""
    return {
        "service": "IDJC MCP Server",
        "version": "1.0.0",
        "agency": "Idaho Department of Juvenile Corrections",
        "endpoints": {
            "health": "/health",
            "capabilities": "/capabilities",
            "execute": "/execute",
        },
    }


if __name__ == "__main__":
    import os
    import uvicorn

    port = int(os.environ.get("MCP_IDJC_PORT", settings.mcp.idjc_port))
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )
