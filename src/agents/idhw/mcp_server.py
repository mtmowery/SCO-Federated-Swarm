"""FastAPI MCP server for IDHW agency operations.

Serves the FastMCP tools over HTTP and provides capability discovery
and health endpoints.
"""

import logging
import uvicorn
from datetime import datetime
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from shared.schemas import AgencyCapability, QueryType
from shared.config import settings
from .tools import mcp
from . import db

logger = logging.getLogger(__name__)


# Request/Response models for direct tool execution
class ToolExecutionRequest(BaseModel):
    """Request to execute a tool."""

    tool_name: str
    parameters: dict[str, Any] = {}


class ToolExecutionResponse(BaseModel):
    """Response from tool execution."""

    success: bool
    result: Any = None
    error: Optional[str] = None


# Create FastAPI app
app = FastAPI(
    title="IDHW MCP Server",
    description="Model Context Protocol server for Idaho Department of Health and Welfare",
    version="1.0.0",
)


# Mount FastMCP server as ASGI middleware
@app.on_event("startup")
async def startup():
    """Initialize on startup."""
    logger.info("IDHW MCP server starting up")


@app.on_event("shutdown")
async def shutdown():
    """Cleanup on shutdown."""
    logger.info("IDHW MCP server shutting down")


@app.get("/health")
async def health_check() -> dict[str, Any]:
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "idhw-mcp",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/capabilities")
async def get_capabilities() -> AgencyCapability:
    """Get IDHW agent capabilities and contract.

    Returns:
        AgencyCapability describing the IDHW agent's features and interfaces
    """
    return AgencyCapability(
        agent_id="idhw-agent-001",
        agency="idhw",
        version="1.0.0",
        description="Idaho Department of Health and Welfare foster care data agent",
        data_domain=["foster_care", "family_relationships", "legal_events"],
        entities=["child", "mother", "father", "person"],
        join_keys=["insight_id", "child_insight_id", "mother_insight_id", "father_insight_id", "ssn"],
        capabilities=[
            "lookup",
            "bulk_lookup",
            "relationship_discovery",
            "aggregation",
            "search",
            "statistics",
        ],
        security_level="confidential",
    )


@app.post("/execute")
async def execute_tool(request: ToolExecutionRequest) -> ToolExecutionResponse:
    """Execute a tool directly (alternative to MCP protocol).

    Args:
        request: Tool execution request with name and parameters

    Returns:
        ToolExecutionResponse with result or error

    Raises:
        HTTPException: If tool not found or execution fails
    """
    tool_name = request.tool_name
    params = request.parameters

    try:
        # Route to correct tool function
        if tool_name == "get_children":
            result = await db.get_all_children()
            return ToolExecutionResponse(success=True, result={"children": result})

        elif tool_name == "get_foster_children":
            result = await db.get_foster_children()
            return ToolExecutionResponse(success=True, result={"foster_children": result})

        elif tool_name == "get_person":
            if "insight_id" not in params:
                raise ValueError("Missing required parameter: insight_id")
            result = await db.get_person_by_insight_id(params["insight_id"])
            return ToolExecutionResponse(success=True, result={"person": result})

        elif tool_name == "get_people_bulk":
            if "insight_ids" not in params:
                raise ValueError("Missing required parameter: insight_ids")
            result = await db.get_people_by_insight_ids(params["insight_ids"])
            return ToolExecutionResponse(success=True, result={"people": result})

        elif tool_name == "get_family_relationships":
            result = await db.get_family_relationships()
            return ToolExecutionResponse(success=True, result={"relationships": result})

        elif tool_name == "get_parent_map":
            result = await db.get_parent_map()
            return ToolExecutionResponse(success=True, result={"parent_map": result})

        elif tool_name == "count_by_end_reason":
            result = await db.count_children_by_end_reason()
            return ToolExecutionResponse(success=True, result={"counts": result})

        elif tool_name == "search_people":
            result = await db.search_people(params)
            return ToolExecutionResponse(success=True, result={"results": result})

        elif tool_name == "get_stats":
            result = await db.get_stats()
            return ToolExecutionResponse(success=True, result={"statistics": result})

        else:
            raise ValueError(f"Unknown tool: {tool_name}")

    except Exception as e:
        logger.error(f"Tool execution failed: {tool_name} - {str(e)}")
        raise HTTPException(
            status_code=400,
            detail=f"Tool execution failed: {str(e)}",
        )


@app.get("/tools")
async def list_tools() -> dict[str, Any]:
    """List all available tools.

    Returns:
        Dictionary describing all available MCP tools
    """
    return {
        "tools": [
            {
                "name": "get_children",
                "description": "Get all children records from IDHW foster care system",
                "parameters": {},
            },
            {
                "name": "get_foster_children",
                "description": "Get all children currently or previously in foster care",
                "parameters": {},
            },
            {
                "name": "get_person",
                "description": "Get a single person record by insight_id",
                "parameters": {"insight_id": {"type": "string", "description": "Global identity identifier"}},
            },
            {
                "name": "get_people_bulk",
                "description": "Get multiple people records by insight_ids (bulk lookup)",
                "parameters": {"insight_ids": {"type": "array", "description": "List of insight_id strings"}},
            },
            {
                "name": "get_family_relationships",
                "description": "Get child-parent relationships (insight_ids only)",
                "parameters": {},
            },
            {
                "name": "get_parent_map",
                "description": "Get mapping of parent insight_ids to their children",
                "parameters": {},
            },
            {
                "name": "count_by_end_reason",
                "description": "Get aggregate counts of children by end_reason",
                "parameters": {},
            },
            {
                "name": "search_people",
                "description": "Search for people using flexible filters",
                "parameters": {
                    "person_type": {"type": "string", "description": "Filter by person type"},
                    "agency_id": {"type": "string", "description": "Filter by agency"},
                    "first_name": {"type": "string", "description": "Partial first name match"},
                    "last_name": {"type": "string", "description": "Partial last name match"},
                    "gender": {"type": "string", "description": "Filter by gender"},
                    "ssn": {"type": "string", "description": "Filter by social security number"},
                    "end_reason": {"type": "string", "description": "Filter by end reason"},
                },
            },
            {
                "name": "get_stats",
                "description": "Get overall statistics about IDHW records",
                "parameters": {},
            },
        ]
    }


def run_server(host: str = "0.0.0.0", port: int = 8001, reload: bool = False):
    """Run the MCP server.

    Args:
        host: Server host
        port: Server port
        reload: Enable auto-reload on code changes
    """
    logger.info(f"Starting IDHW MCP server on {host}:{port}")
    uvicorn.run(
        app,
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )


if __name__ == "__main__":
    import os
    port = int(os.environ.get("MCP_IDHW_PORT", settings.mcp.idhw_port))
    run_server(host="0.0.0.0", port=port)
