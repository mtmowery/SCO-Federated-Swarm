"""
Pydantic schemas for Idaho Federated AI Swarm.

Defines request/response contracts and internal state models for the swarm.
"""

from uuid import UUID, uuid4
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Optional, TypedDict
from pydantic import BaseModel, Field, ConfigDict
import operator


class QueryType(str, Enum):
    """Types of agent queries."""

    AGGREGATE = "aggregate"
    LOOKUP = "lookup"
    RELATIONSHIP = "relationship"
    BULK = "bulk"


class QueryIntent(str, Enum):
    """Intent classification for queries."""

    CROSS_AGENCY = "cross_agency"
    SINGLE_AGENCY = "single_agency"
    STATISTICS = "statistics"
    LOOKUP = "lookup"
    RELATIONSHIP = "relationship"


class AgencyName(str, Enum):
    """Idaho agencies in the federated swarm."""

    IDHW = "idhw"  # Idaho Department of Health and Welfare (foster care)
    IDJC = "idjc"  # Idaho Department of Juvenile Corrections
    IDOC = "idoc"  # Idaho Department of Corrections


class ResponseStatus(str, Enum):
    """Response status values."""

    SUCCESS = "success"
    ERROR = "error"
    PARTIAL = "partial"


class AgentQuery(BaseModel):
    """Agent query request schema."""

    model_config = ConfigDict(json_schema_extra={"example": {}})

    request_id: UUID = Field(
        default_factory=uuid4, description="Unique request identifier"
    )
    query_type: QueryType = Field(description="Type of query")
    question: str = Field(min_length=1, description="Natural language question")
    filters: dict[str, Any] = Field(
        default_factory=dict, description="Query filters for agencies/records"
    )
    join_keys: Optional[list[str]] = Field(
        default=None, description="Keys for cross-agency joins"
    )
    security_context: dict[str, Any] = Field(
        default_factory=dict, description="Security context with user/role info"
    )


class Provenance(BaseModel):
    """Provenance information for response data."""

    model_config = ConfigDict(json_schema_extra={"example": {}})

    agency: AgencyName = Field(description="Source agency")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    query_type: QueryType = Field(description="Query type used")
    schema_version: str = Field(default="1.0", description="Data schema version")


class AgentResponse(BaseModel):
    """Agent response schema."""

    model_config = ConfigDict(json_schema_extra={"example": {}})

    request_id: UUID = Field(description="Matches request_id from query")
    status: ResponseStatus = Field(description="Response status")
    data: dict[str, Any] = Field(
        default_factory=dict, description="Query result data"
    )
    provenance: list[Provenance] = Field(
        default_factory=list, description="Data provenance chain"
    )
    confidence: float = Field(
        ge=0.0,
        le=1.0,
        default=1.0,
        description="Confidence score for results",
    )
    security_tags: list[str] = Field(
        default_factory=list, description="Security classification tags"
    )
    error_message: Optional[str] = Field(default=None, description="Error details if failed")


def _merge_dicts(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    """Merge two dicts, preferring non-empty values from b."""
    merged = {**a}
    for k, v in b.items():
        if v:  # only overwrite if b has a truthy value
            merged[k] = v
    return merged


class InsightState(TypedDict, total=False):
    """Internal state for LangGraph orchestration.

    Uses Annotated reducers on list/dict fields so parallel agency
    nodes can write without triggering INVALID_CONCURRENT_GRAPH_UPDATE.
    """

    # Query details — set once, read-only after intent node
    question: str
    intent: QueryIntent
    plan: list[str]

    # Agency involvement
    agencies: list[AgencyName]

    # Raw agency data — each written by exactly one executor
    idhw_data: dict[str, Any]
    idjc_data: dict[str, Any]
    idoc_data: dict[str, Any]

    # Cross-agency parent linkage — extracted from idhw_data before IDJC/IDOC fan-out
    # parent_ids: flat list of all unique parent insight_ids from foster children
    # child_to_parents: map of child_insight_id -> list of parent insight_ids
    parent_ids: list[str]
    child_to_parents: dict[str, list]

    # Cross-agency resolution
    identity_matches: dict[str, Any]

    # Reasoning results
    reasoning_result: dict[str, Any]
    answer: str
    confidence: float

    # Audit trail — these are appended from parallel nodes, need reducers
    sources: Annotated[list[str], operator.add]
    errors: Annotated[list[str], operator.add]
    execution_trace: Annotated[list[str], operator.add]


class MatchedIdentity(BaseModel):
    """Cross-agency identity match result."""

    model_config = ConfigDict(json_schema_extra={"example": {}})

    insight_id: str = Field(description="Global insight identifier")
    agencies_found: list[AgencyName] = Field(
        description="Agencies where this identity was found"
    )
    child_insight_id: Optional[str] = Field(
        default=None, description="IDHW child identity (if applicable)"
    )
    mother_insight_id: Optional[str] = Field(
        default=None, description="IDHW mother identity (if applicable)"
    )
    father_insight_id: Optional[str] = Field(
        default=None, description="IDHW father identity (if applicable)"
    )
    confidence: float = Field(
        ge=0.0, le=1.0, description="Match confidence score"
    )
    match_fields: list[str] = Field(
        description="Fields used for matching (SSN, DOB, name, etc)"
    )


class AgencyCapability(BaseModel):
    """Agent capability descriptor."""

    model_config = ConfigDict(json_schema_extra={"example": {}})

    agent_id: str = Field(description="Unique agent identifier")
    agency: AgencyName = Field(description="Agency name")
    version: str = Field(description="Agent version")
    description: str = Field(description="Agent description")
    data_domain: list[str] = Field(description="Data domains handled")
    entities: list[str] = Field(description="Entity types managed")
    join_keys: list[str] = Field(
        description="Keys available for joins (e.g., insight_id, SSN)"
    )
    capabilities: list[str] = Field(
        description="Specific capabilities (e.g., lookup, aggregate, relationship)"
    )
    security_level: str = Field(
        default="confidential", description="Security classification"
    )
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class BulkQueryRequest(BaseModel):
    """Bulk query request for batch operations."""

    model_config = ConfigDict(json_schema_extra={"example": {}})

    request_id: UUID = Field(
        default_factory=uuid4, description="Unique request identifier"
    )
    queries: list[AgentQuery] = Field(min_items=1, description="Batch of queries")
    timeout: int = Field(default=300, description="Timeout in seconds")


class BulkQueryResponse(BaseModel):
    """Bulk query response."""

    model_config = ConfigDict(json_schema_extra={"example": {}})

    request_id: UUID = Field(description="Matches bulk request")
    responses: list[AgentResponse] = Field(description="Responses in order")
    total_time_ms: float = Field(description="Total processing time")
    success_count: int = Field(description="Successful queries")
    error_count: int = Field(description="Failed queries")
