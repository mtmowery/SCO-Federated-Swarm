"""
FastAPI MCP server for IDOC agency module.

Exposes IDOC tools via HTTP with capability discovery and health checks.
Designed to run on port 8003 in the federated swarm architecture.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.shared.config import settings
from src.shared.database import DatabaseManager, close_all_connections
from .tools import mcp

logger = logging.getLogger(__name__)


# Request/Response schemas
class CapabilityResponse(BaseModel):
    """MCP capability descriptor response."""

    agent_id: str
    agency: str
    version: str
    description: str
    data_domain: list[str]
    entities: list[str]
    join_keys: list[str]
    capabilities: list[str]
    security_level: str


class ExecuteRequest(BaseModel):
    """MCP tool execution request."""

    tool_name: str
    params: dict


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    agency: str
    database: str
    message: str


# Lifecycle events
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown."""
    # Startup: test database connections
    logger.info("IDOC MCP Server starting up...")
    try:
        engine = await DatabaseManager.get_pg_engine("idoc")
        logger.info("PostgreSQL connection pool initialized for IDOC")
    except Exception as e:
        logger.error(f"Failed to initialize IDOC database: {e}")
        raise

    yield

    # Shutdown: close all connections
    logger.info("IDOC MCP Server shutting down...")
    await close_all_connections()
    logger.info("All database connections closed")


# Create FastAPI app
app = FastAPI(
    title="IDOC Agency MCP Server",
    description="FastMCP server for Idaho Department of Corrections data access",
    version="1.0.0",
    lifespan=lifespan,
)


# Error handlers
@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "agency": "idoc",
            "message": "Internal server error",
            "detail": str(exc) if settings.debug else "Server error",
        },
    )


# Endpoints
@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Health check endpoint.

    Returns:
        Health status with database connection status
    """
    try:
        engine = await DatabaseManager.get_pg_engine("idoc")
        db_status = "connected"
    except Exception as e:
        db_status = f"disconnected: {str(e)}"
        logger.warning(f"Database health check failed: {e}")

    return HealthResponse(
        status="healthy" if db_status == "connected" else "degraded",
        agency="idoc",
        database=db_status,
        message="IDOC MCP Server operational",
    )


@app.get("/capabilities", response_model=CapabilityResponse)
async def get_capabilities() -> CapabilityResponse:
    """
    Get IDOC agent capabilities.

    Returns:
        Capability descriptor for tool discovery
    """
    return CapabilityResponse(
        agent_id="idoc-agent-1",
        agency="idoc",
        version="1.0.0",
        description="Idaho Department of Corrections - Adult Incarceration & Sentencing Records",
        data_domain=["adult_corrections", "sentencing", "incarceration"],
        entities=["offender", "sentence", "offense", "incarceration_record"],
        join_keys=["insight_id", "ofndr_num", "ssn_nbr"],
        capabilities=[
            "lookup_person",
            "bulk_lookup",
            "incarceration_check",
            "incarceration_count",
            "active_offenders",
            "offense_aggregation",
            "status_aggregation",
            "advanced_search",
        ],
        security_level="confidential",
    )


@app.post("/execute")
async def execute_tool(request: ExecuteRequest) -> dict:
    """
    Execute an MCP tool.

    Args:
        request: ExecuteRequest with tool_name and params

    Returns:
        Tool execution result

    Raises:
        HTTPException: If tool not found or execution fails
    """
    logger.info(f"Executing tool: {request.tool_name} with params: {request.params}")

    # Map tool names to MCP tool functions
    tool_map = {
        "get_sentences": mcp.tools[0].func if mcp.tools else None,
        "get_person": mcp.tools[1].func if len(mcp.tools) > 1 else None,
        "get_people_bulk": mcp.tools[2].func if len(mcp.tools) > 2 else None,
        "check_incarceration": mcp.tools[3].func if len(mcp.tools) > 3 else None,
        "count_incarcerated": mcp.tools[4].func if len(mcp.tools) > 4 else None,
        "get_active_offenders": mcp.tools[5].func if len(mcp.tools) > 5 else None,
        "get_offense_summary": mcp.tools[6].func if len(mcp.tools) > 6 else None,
        "count_by_status": mcp.tools[7].func if len(mcp.tools) > 7 else None,
        "search_sentences": mcp.tools[8].func if len(mcp.tools) > 8 else None,
    }

    # Import the functions directly to avoid complex MCP reflection
    from . import db

    direct_tools = {
        "get_sentences": db.get_all_sentences,
        "get_person": db.get_person_by_insight_id,
        "get_people_bulk": db.get_people_by_insight_ids,
        "check_incarceration": db.check_incarceration,
        "count_incarcerated": db.count_incarcerated_from_ids,
        "get_active_offenders": db.get_active_offenders,
        "get_offense_summary": db.get_offense_summary,
        "count_by_status": db.count_by_status,
        "search_sentences": db.search_sentences,
    }

    if request.tool_name not in direct_tools:
        raise HTTPException(
            status_code=404,
            detail=f"Tool not found: {request.tool_name}. "
            f"Available tools: {list(direct_tools.keys())}",
        )

    try:
        tool_func = direct_tools[request.tool_name]
        result = await tool_func(**request.params)

        return {
            "status": "success",
            "tool": request.tool_name,
            "result": result,
        }
    except TypeError as e:
        logger.error(f"Invalid parameters for {request.tool_name}: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid parameters: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Tool execution failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Tool execution failed: {str(e)}",
        )


@app.get("/tools")
async def list_tools() -> dict:
    """
    List available MCP tools.

    Returns:
        Dict of tool names and descriptions
    """
    tools = {
        "get_sentences": "Retrieve paginated IDOC sentence records",
        "get_person": "Get all sentence records for a specific person",
        "get_people_bulk": "Bulk lookup sentence records for multiple people",
        "check_incarceration": "Check which people have active sentences",
        "count_incarcerated": "Count people with active sentences",
        "get_active_offenders": "Get list of all active offenders",
        "get_offense_summary": "Aggregate statistics by crime group",
        "count_by_status": "Count sentences by status",
        "search_sentences": "Advanced search with flexible filtering",
    }

    return {"agency": "idoc", "tools": tools, "count": len(tools)}


# Root endpoint
@app.get("/")
async def root() -> dict:
    """Root endpoint with server info."""
    return {
        "agency": "idoc",
        "name": "IDOC Agency MCP Server",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "capabilities": "/capabilities",
            "tools": "/tools",
            "execute": "/execute",
        },
    }


if __name__ == "__main__":
    import uvicorn

    config = settings.mcp

    uvicorn.run(
        app,
        host=config.idoc_host,
        port=config.idoc_port,
        log_level="info",
    )
