"""
Cross-Agency Reasoning Agent

Builds ephemeral relationship graphs from multi-agency data and computes
cross-agency answers. This is the core intelligence layer that joins
data from IDHW, IDJC, and IDOC without either agency exposing raw data
to the other.

Patterns:
- Ephemeral Knowledge Graph: temporary in-memory graph per query
- Evidence Fusion: weighted combination of multi-source results
- Confidence Scoring: data completeness and match quality assessment
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from shared.schemas import InsightState, QueryIntent, AgencyName

logger = logging.getLogger(__name__)


@dataclass
class RelationshipEdge:
    """An edge in the ephemeral relationship graph."""
    source_id: str
    target_id: str
    relationship_type: str  # IN_AGENCY, PARENT_OF
    source_agency: str
    confidence: float = 1.0
    metadata: dict = field(default_factory=dict)


@dataclass
class EphemeralGraph:
    """
    Temporary in-memory relationship graph built per query.
    This is the 'Palantir-style' ephemeral knowledge graph
    described in the architecture docs.
    """
    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    edges: list[RelationshipEdge] = field(default_factory=list)
    _adjacency: dict[str, list[RelationshipEdge]] = field(default_factory=lambda: defaultdict(list))

    def add_node(self, insight_id: str, attributes: dict[str, Any]) -> None:
        if insight_id in self.nodes:
            self.nodes[insight_id].update(attributes)
        else:
            self.nodes[insight_id] = attributes

    def add_edge(self, edge: RelationshipEdge) -> None:
        self.edges.append(edge)
        self._adjacency[edge.source_id].append(edge)

    def get_neighbors(self, insight_id: str, relationship_type: str | None = None) -> list[RelationshipEdge]:
        edges = self._adjacency.get(insight_id, [])
        if relationship_type:
            return [e for e in edges if e.relationship_type == relationship_type]
        return edges

    def get_all_of_type(self, relationship_type: str) -> list[RelationshipEdge]:
        return [e for e in self.edges if e.relationship_type == relationship_type]

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    @property
    def edge_count(self) -> int:
        return len(self.edges)


class CrossAgencyReasoner:
    """
    The Cross-Agency Reasoning Agent.

    Builds temporary relationship graphs from agency data and
    computes cross-agency answers using graph traversal and
    evidence fusion.
    """

    def __init__(self) -> None:
        self.graph = EphemeralGraph()

    def reset(self) -> None:
        """Clear the ephemeral graph for a new query."""
        self.graph = EphemeralGraph()

    # ── Graph Construction ──────────────────────────────────────

    def build_family_graph(self, idhw_data: dict) -> None:
        """
        Build family relationship subgraph from IDHW data using minimalist schema.
        """
        relationships = idhw_data.get("family_relationships", idhw_data.get("children", []))
        if isinstance(relationships, dict) and "records" in relationships:
            relationships = relationships["records"]

        # Ensure IDHW Agency node
        self.graph.add_node("IDHW", {"type": "Agency", "agency_id": "IDHW"})

        for record in relationships:
            child_id = record.get("child_insight_id") or record.get("insight_id")
            mother_id = record.get("mother_insight_id")
            father_id = record.get("father_insight_id")

            if not child_id:
                continue

            # Add child Person node
            self.graph.add_node(child_id, {
                "type": "Person",
                "insight_id": child_id,
                "gender": record.get("gender"),
                "dob": record.get("dob"), # dob split into year/month in loader, here keeping for context
                "dob_year": record.get("dob_year"),
                "dob_month": record.get("dob_month"),
            })

            # Link child to IDHW Agency
            self.graph.add_edge(RelationshipEdge(
                source_id="IDHW",
                target_id=child_id,
                relationship_type="IN_AGENCY",
                source_agency="IDHW"
            ))

            # Add mother link
            if mother_id:
                self.graph.add_node(mother_id, {"type": "Person", "insight_id": mother_id})
                self.graph.add_edge(RelationshipEdge(
                    source_id=mother_id,
                    target_id=child_id,
                    relationship_type="PARENT_OF",
                    source_agency="IDHW",
                ))

            # Add father link
            if father_id:
                self.graph.add_node(father_id, {"type": "Person", "insight_id": father_id})
                self.graph.add_edge(RelationshipEdge(
                    source_id=father_id,
                    target_id=child_id,
                    relationship_type="PARENT_OF",
                    source_agency="IDHW",
                ))

    def add_incarceration_data(self, idoc_data: dict) -> None:
        """
        Overlay IDOC agency links onto Persons.
        """
        incarcerated_ids = set()
        if "incarcerated_ids" in idoc_data:
            incarcerated_ids = set(idoc_data["incarcerated_ids"])
        elif "records" in idoc_data:
            for record in idoc_data["records"]:
                incarcerated_ids.add(record.get("insight_id"))

        # Ensure IDOC Agency node
        self.graph.add_node("IDOC", {"type": "Agency", "agency_id": "IDOC"})

        for insight_id in incarcerated_ids:
            if not insight_id:
                continue
            self.graph.add_node(insight_id, {"type": "Person", "insight_id": insight_id})
            self.graph.add_edge(RelationshipEdge(
                source_id="IDOC",
                target_id=insight_id,
                relationship_type="IN_AGENCY",
                source_agency="IDOC",
            ))

    def add_juvenile_data(self, idjc_data: dict) -> None:
        """
        Overlay IDJC agency links onto Persons.
        """
        juvenile_ids = set()
        if "juvenile_ids" in idjc_data:
            juvenile_ids = set(idjc_data["juvenile_ids"])
        elif "records" in idjc_data:
            for record in idjc_data["records"]:
                juvenile_ids.add(record.get("insight_id"))

        # Ensure IDJC Agency node
        self.graph.add_node("IDJC", {"type": "Agency", "agency_id": "IDJC"})

        for insight_id in juvenile_ids:
            if not insight_id:
                continue
            self.graph.add_node(insight_id, {"type": "Person", "insight_id": insight_id})
            self.graph.add_edge(RelationshipEdge(
                source_id="IDJC",
                target_id=insight_id,
                relationship_type="IN_AGENCY",
                source_agency="IDJC",
            ))

    # ── Query Computations ──────────────────────────────────────

    def count_children_with_incarcerated_parents(self) -> dict[str, Any]:
        """
        Traversal: Agency(IDHW) -> Child -> PARENT_OF <- Parent <- Agency(IDOC)
        """
        foster_ids = {e.target_id for e in self.graph.get_neighbors("IDHW", "IN_AGENCY")}
        idoc_ids = {e.target_id for e in self.graph.get_neighbors("IDOC", "IN_AGENCY")}

        found = []
        for child_id in foster_ids:
            # Look for edges pointing TO this child (to find parents)
            # Adjacency is source->target. Parents are sources of PARENT_OF
            for parent_id, attrs in self.graph.nodes.items():
                if parent_id == child_id or attrs.get("type") != "Person":
                    continue
                
                # Check if this node is a parent of the child
                parent_of_edges = self.graph.get_neighbors(parent_id, "PARENT_OF")
                for e in parent_of_edges:
                    if e.target_id == child_id and parent_id in idoc_ids:
                        found.append({"child_id": child_id, "parent_id": parent_id})
                        break

        return {
            "count": len(found),
            "total_foster": len(foster_ids),
            "details": found,
        }

    def count_foster_kids_with_foster_parents_in_idoc(self) -> dict[str, Any]:
        """
        Traversal: Agency(IDHW) -> Child <- PARENT_OF - Parent <- Agency(IDOC)
                                                            ^
                                                            |-- Agency(IDHW)
        """
        foster_ids = {e.target_id for e in self.graph.get_neighbors("IDHW", "IN_AGENCY")}
        idoc_ids = {e.target_id for e in self.graph.get_neighbors("IDOC", "IN_AGENCY")}

        found = []
        for child_id in foster_ids:
            for parent_id, attrs in self.graph.nodes.items():
                if parent_id == child_id or attrs.get("type") != "Person":
                    continue
                parent_of_edges = self.graph.get_neighbors(parent_id, "PARENT_OF")
                for e in parent_of_edges:
                    if e.target_id == child_id and parent_id in idoc_ids and parent_id in foster_ids:
                        found.append({"child_id": child_id, "parent_id": parent_id})
                        break

        return {
            "count": len(found),
            "total_foster": len(foster_ids),
            "details": found,
        }

    def count_incarcerated_with_foster_children(self) -> dict[str, Any]:
        """
        Traversal: Agency(IDOC) -> Parent -> PARENT_OF -> Child <- Agency(IDHW)
        """
        idoc_ids = {e.target_id for e in self.graph.get_neighbors("IDOC", "IN_AGENCY")}
        foster_ids = {e.target_id for e in self.graph.get_neighbors("IDHW", "IN_AGENCY")}

        found = defaultdict(list)
        for parent_id in idoc_ids:
            parent_of_edges = self.graph.get_neighbors(parent_id, "PARENT_OF")
            for e in parent_of_edges:
                if e.target_id in foster_ids:
                    found[parent_id].append(e.target_id)

        return {
            "count": len(found),
            "total_incarcerated": len(idoc_ids),
            "details": [{"parent_id": pid, "children": cids} for pid, cids in found.items()],
        }

    def count_foster_youth_with_juvenile_record(self) -> dict[str, Any]:
        """
        Traversal: Agency(IDHW) -> Person <- Agency(IDJC)
        """
        foster_ids = {e.target_id for e in self.graph.get_neighbors("IDHW", "IN_AGENCY")}
        idjc_ids = {e.target_id for e in self.graph.get_neighbors("IDJC", "IN_AGENCY")}

        overlap = foster_ids.intersection(idjc_ids)

        return {
            "count": len(overlap),
            "total_foster": len(foster_ids),
            "ids": list(overlap),
        }

    def count_juveniles_with_adult_records(self) -> dict[str, Any]:
        """
        Traversal: Agency(IDJC) -> Person <- Agency(IDOC)
        """
        idjc_ids = {e.target_id for e in self.graph.get_neighbors("IDJC", "IN_AGENCY")}
        idoc_ids = {e.target_id for e in self.graph.get_neighbors("IDOC", "IN_AGENCY")}

        overlap = idjc_ids.intersection(idoc_ids)

        return {
            "count": len(overlap),
            "total_juvenile": len(idjc_ids),
            "ids": list(overlap),
        }

    def compute_family_risk_network(self, insight_id: str) -> dict[str, Any]:
        """
        Build a risk profile for a family network around a given person.
        Returns all connected individuals and their cross-agency status.
        """
        visited = set()
        network = []

        def _traverse(nid: str, depth: int = 0) -> None:
            if nid in visited or depth > 3:
                return
            visited.add(nid)
            node_data = self.graph.nodes.get(nid, {})
            edges = self.graph._adjacency.get(nid, [])

            network.append({
                "insight_id": nid,
                "attributes": node_data,
                "connections": [
                    {"target": e.target_id, "type": e.relationship_type}
                    for e in edges
                ],
                "depth": depth,
            })

            for edge in edges:
                _traverse(edge.target_id, depth + 1)

        _traverse(insight_id)
        return {
            "root": insight_id,
            "network_size": len(network),
            "members": network,
        }

    # ── Evidence Fusion ─────────────────────────────────────────

    def compute_confidence(self, result: dict) -> float:
        """
        Compute confidence score for a cross-agency result.

        Factors:
        - Data completeness (are all expected agencies represented?)
        - Graph density (enough edges to support the claim?)
        - Match quality (are joins deterministic via insight_id?)
        """
        agencies_present = set()
        for edge in self.graph.edges:
            agencies_present.add(edge.source_agency)

        # Completeness: what fraction of the graph came from real data
        completeness = len(agencies_present) / 3.0  # 3 agencies total

        # Since we use insight_id (deterministic), match quality is 1.0
        match_quality = 1.0

        # Density: do we have enough edges relative to nodes
        density = min(1.0, self.graph.edge_count / max(self.graph.node_count, 1))

        confidence = (completeness * 0.4) + (match_quality * 0.4) + (density * 0.2)
        return round(min(confidence, 1.0), 3)


# ── LangGraph Node Function ────────────────────────────────────

async def reasoning_node(state: InsightState) -> dict:
    """
    LangGraph node: Cross-Agency Reasoning.

    Builds ephemeral graph from agency data in state, computes the
    cross-agency answer, and returns a PARTIAL STATE DICT — never
    mutates the state object directly (violates LangGraph's reducer contract).
    """
    reasoner = CrossAgencyReasoner()
    errors: list[str] = []

    idhw_data = state.get("idhw_data", {})
    idoc_data = state.get("idoc_data", {})
    idjc_data = state.get("idjc_data", {})

    # Build ephemeral graph from available agency data
    if idhw_data:
        try:
            reasoner.build_family_graph(idhw_data)
        except Exception as e:
            errors.append(f"IDHW graph build error: {e}")
            logger.error(f"Failed to build IDHW family graph: {e}")

    if idoc_data:
        try:
            reasoner.add_incarceration_data(idoc_data)
        except Exception as e:
            errors.append(f"IDOC graph overlay error: {e}")
            logger.error(f"Failed to overlay IDOC data: {e}")

    if idjc_data:
        try:
            reasoner.add_juvenile_data(idjc_data)
        except Exception as e:
            errors.append(f"IDJC graph overlay error: {e}")
            logger.error(f"Failed to overlay IDJC data: {e}")

    # Determine which computation to run based on the question and plan
    question = state.get("question", "").lower()
    plan = state.get("plan", [])
    intent = state.get("intent")
    agencies = state.get("agencies", [])
    result: dict[str, Any] = {}
    
    # Safely convert to string values robustly if passed as enum or string via state checkpointing
    intent_raw = getattr(intent, "value", intent)
    intent_val = str(intent_raw).split(".")[-1].lower()
    
    agency_vals = []
    for a in agencies:
        val = getattr(a, "value", a)
        agency_vals.append(str(val).split(".")[-1].lower())

    if (intent_val in ("statistics", "single_agency", "lookup") or "breakdown" in question or "murder" in question or "theft" in question) and len(agency_vals) == 1:
        agency = agency_vals[0]
        count = 0
        breakdown: dict[str, Any] = {}
        total_records = 0

        if agency == "idhw" and idhw_data:
            stats = idhw_data.get("statistics", {})
            if stats:
                # Use accurate DB-level counts from get_stats
                count = stats.get("children", 0) or stats.get("total_records", 0)
                breakdown = stats
            else:
                count = len(idhw_data.get("child_records", []))
                if not count:
                    count = len(idhw_data.get("family_relationships", []))

        elif agency == "idjc" and idjc_data:
            stats = idjc_data.get("statistics", {})
            if stats:
                # Use accurate DB-level counts: total unique people
                count = stats.get("total_people", 0)
                total_records = stats.get("total_records", 0)
                breakdown = stats.get("by_status", {})
                
                # Check for intercepted offense breakdown outputs
                if "offense_breakdown" in stats:
                    offense_stats = stats["offense_breakdown"]
                    breakdown = offense_stats.get("by_type", {})
                    count = offense_stats.get("total_people", 0)
                elif "top_offenders" in stats:
                    breakdown = {"top_offenders": stats["top_offenders"]}
            else:
                # Fallback: count unique insight_ids from commitments
                seen_ids = set()
                for r in idjc_data.get("commitments", []):
                    if isinstance(r, dict) and r.get("insight_id"):
                        seen_ids.add(r["insight_id"])
                count = len(seen_ids) if seen_ids else len(idjc_data.get("juvenile_ids", []))

        elif agency == "idoc" and idoc_data:
            stats = idoc_data.get("statistics", {})
            if stats:
                count = stats.get("total_people", 0)
                total_records = stats.get("total_records", 0)
                breakdown = stats.get("by_status", {})
                
                # Check for intercepted offense breakdown outputs
                if "offense_breakdown" in stats:
                    offense_stats = stats["offense_breakdown"]
                    breakdown = offense_stats.get("by_type", {})
                    count = offense_stats.get("total_people", 0)
            else:
                seen_ids = set()
                for r in idoc_data.get("inmates", []):
                    if isinstance(r, dict) and r.get("insight_id"):
                        seen_ids.add(r["insight_id"])
                count = len(seen_ids) if seen_ids else len(idoc_data.get("incarcerated_ids", []))

        result = {
            "query_type": "single_agency_statistics",
            "count": count,
            "total_records": total_records,
            "breakdown": breakdown,
            "agency": agency,
        }

    elif _is_foster_parents_in_idoc_query(question, plan):
        result = reasoner.count_foster_kids_with_foster_parents_in_idoc()
        result["query_type"] = "foster_kids_with_foster_parents_in_idoc"

    elif _is_foster_incarceration_query(question, plan):
        result = reasoner.count_children_with_incarcerated_parents()
        result["query_type"] = "foster_children_with_incarcerated_parents"

    elif _is_incarcerated_with_children_query(question, plan):
        result = reasoner.count_incarcerated_with_foster_children()
        result["query_type"] = "incarcerated_with_foster_children"

    elif _is_foster_juvenile_query(question, plan):
        result = reasoner.count_foster_youth_with_juvenile_record()
        result["query_type"] = "foster_youth_with_juvenile_record"

    elif _is_juvenile_incarceration_query(question, plan):
        result = reasoner.count_juveniles_with_adult_records()
        result["query_type"] = "juvenile_youth_with_adult_record"

    else:
        # Generic: try all and return whichever has data
        foster_inc = reasoner.count_children_with_incarcerated_parents()
        if foster_inc.get("count", 0) > 0:
            result = foster_inc
            result["query_type"] = "foster_children_with_incarcerated_parents"
        else:
            result = {
                "query_type": "unknown",
                "note": "Could not determine specific computation from query.",
            }

    # Add confidence and graph metadata
    if result.get("query_type") == "single_agency_statistics":
        confidence = 0.95
    else:
        confidence = reasoner.compute_confidence(result)
        
    result["confidence"] = confidence
    result["graph_stats"] = {
        "nodes": reasoner.graph.node_count,
        "edges": reasoner.graph.edge_count,
    }
    result["timestamp"] = datetime.now(timezone.utc).isoformat()

    # Collect source agency names from graph edges
    new_sources = list({edge.source_agency for edge in reasoner.graph.edges})

    trace_msg = (
        f"[Reasoning] Ephemeral graph: {reasoner.graph.node_count} nodes, "
        f"{reasoner.graph.edge_count} edges. "
        f"Result: {result.get('query_type')} "
        f"count={result.get('count', 'N/A')}, confidence={confidence}"
    )
    logger.info(trace_msg)

    # ── Return a partial dict — let LangGraph reducers merge ────────
    return {
        "reasoning_result": result,
        "confidence": confidence,
        "sources": new_sources,          # Annotated[list, operator.add] reducer appends
        "errors": errors,                # Annotated[list, operator.add] reducer appends
        "execution_trace": [trace_msg],  # Annotated[list, operator.add] reducer appends
    }


# ── Helper functions for query routing ──────────────────────────

def _is_foster_parents_in_idoc_query(question: str, plan: list) -> bool:
    text = question + " " + " ".join(plan).lower()
    return "foster" in text and "parent" in text and ("idoc" in text or "adult" in text) and "also" in text

def _is_foster_incarceration_query(question: str, plan: list) -> bool:
    foster_terms = {"foster", "child", "children", "kid"}
    prison_terms = {"prison", "incarcerat", "jail", "locked up"}
    text = question + " " + " ".join(plan).lower()
    return any(t in text for t in foster_terms) and any(t in text for t in prison_terms)


def _is_incarcerated_with_children_query(question: str, plan: list) -> bool:
    text = question + " " + " ".join(plan).lower()
    return "incarcerat" in text and ("have children" in text or "have kids" in text or "with children" in text)


def _is_foster_juvenile_query(question: str, plan: list) -> bool:
    foster_terms = {"foster"}
    juvenile_terms = {"juvenile", "detention", "youth"}
    text = question + " " + " ".join(plan).lower()
    return any(t in text for t in foster_terms) and any(t in text for t in juvenile_terms)


def _is_juvenile_incarceration_query(question: str, plan: list) -> bool:
    juvenile_terms = {"juvenile", "detention", "youth", "idjc"}
    prison_terms = {"prison", "incarcerat", "jail", "locked up", "adult", "idoc"}
    text = question + " " + " ".join(plan).lower()
    return any(t in text for t in juvenile_terms) and any(t in text for t in prison_terms)
