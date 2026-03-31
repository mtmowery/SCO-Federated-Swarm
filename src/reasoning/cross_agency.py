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

from shared.schemas import InsightState

logger = logging.getLogger(__name__)


@dataclass
class RelationshipEdge:
    """An edge in the ephemeral relationship graph."""
    source_id: str
    target_id: str
    relationship_type: str  # HAS_MOTHER, HAS_FATHER, INCARCERATED, JUVENILE_RECORD
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
        Build family relationship subgraph from IDHW data.

        Expects idhw_data to contain 'children' or 'family_relationships'
        with child/mother/father insight_id mappings.
        """
        relationships = idhw_data.get("family_relationships", idhw_data.get("children", []))

        if isinstance(relationships, dict) and "records" in relationships:
            relationships = relationships["records"]

        for record in relationships:
            child_id = record.get("child_insight_id") or record.get("insight_id")
            mother_id = record.get("mother_insight_id")
            father_id = record.get("father_insight_id")

            if not child_id:
                continue

            # Add child node
            self.graph.add_node(child_id, {
                "type": "child",
                "agency": "IDHW",
                "first_name": record.get("first_name"),
                "last_name": record.get("last_name"),
                "dob": record.get("dob"),
                "start_care_date": record.get("start_care_date"),
                "end_care_date": record.get("end_care_date"),
                "end_reason": record.get("end_reason"),
            })

            # Add mother edge
            if mother_id:
                self.graph.add_node(mother_id, {
                    "type": "parent",
                    "role": "mother",
                    "agency": "IDHW",
                })
                self.graph.add_edge(RelationshipEdge(
                    source_id=child_id,
                    target_id=mother_id,
                    relationship_type="HAS_MOTHER",
                    source_agency="IDHW",
                ))

            # Add father edge
            if father_id:
                self.graph.add_node(father_id, {
                    "type": "parent",
                    "role": "father",
                    "agency": "IDHW",
                })
                self.graph.add_edge(RelationshipEdge(
                    source_id=child_id,
                    target_id=father_id,
                    relationship_type="HAS_FATHER",
                    source_agency="IDHW",
                ))

    def add_incarceration_data(self, idoc_data: dict) -> None:
        """
        Overlay incarceration status onto the graph from IDOC data.

        Expects idoc_data with 'incarcerated_ids' list or 'sentences' records.
        """
        incarcerated_ids = set()

        # Handle different response formats
        if "incarcerated_ids" in idoc_data:
            incarcerated_ids = set(idoc_data["incarcerated_ids"])
        elif "records" in idoc_data:
            for record in idoc_data["records"]:
                status = record.get("sent_status", "").upper()
                if status not in ("DISCHARGED", ""):
                    incarcerated_ids.add(record.get("insight_id"))
        elif "results" in idoc_data:
            incarcerated_ids = set(idoc_data["results"])

        for insight_id in incarcerated_ids:
            if not insight_id:
                continue
            self.graph.add_node(insight_id, {
                "incarcerated": True,
                "agency": "IDOC",
            })
            self.graph.add_edge(RelationshipEdge(
                source_id=insight_id,
                target_id=insight_id,
                relationship_type="INCARCERATED",
                source_agency="IDOC",
            ))

    def add_juvenile_data(self, idjc_data: dict) -> None:
        """
        Overlay juvenile commitment data onto the graph from IDJC.

        Expects idjc_data with 'juvenile_ids' list or 'commitments' records.
        """
        juvenile_ids = set()

        if "juvenile_ids" in idjc_data:
            juvenile_ids = set(idjc_data["juvenile_ids"])
        elif "records" in idjc_data:
            for record in idjc_data["records"]:
                juvenile_ids.add(record.get("insight_id"))
        elif "results" in idjc_data:
            juvenile_ids = set(idjc_data["results"])

        for insight_id in juvenile_ids:
            if not insight_id:
                continue
            self.graph.add_node(insight_id, {
                "juvenile_record": True,
                "agency": "IDJC",
            })
            self.graph.add_edge(RelationshipEdge(
                source_id=insight_id,
                target_id=insight_id,
                relationship_type="JUVENILE_RECORD",
                source_agency="IDJC",
            ))

    # ── Query Computations ──────────────────────────────────────

    def count_children_with_incarcerated_parents(self) -> dict[str, Any]:
        """
        Core cross-agency query: How many foster children have
        at least one incarcerated parent?

        Traversal: Child -> HAS_MOTHER/HAS_FATHER -> Parent -> INCARCERATED
        """
        incarcerated_nodes = {
            e.source_id
            for e in self.graph.get_all_of_type("INCARCERATED")
        }

        children_with_incarcerated = []
        total_children = 0

        for node_id, attrs in self.graph.nodes.items():
            if attrs.get("type") != "child":
                continue
            total_children += 1

            parent_edges = (
                self.graph.get_neighbors(node_id, "HAS_MOTHER") +
                self.graph.get_neighbors(node_id, "HAS_FATHER")
            )

            for edge in parent_edges:
                if edge.target_id in incarcerated_nodes:
                    children_with_incarcerated.append({
                        "child_id": node_id,
                        "parent_id": edge.target_id,
                        "relationship": edge.relationship_type,
                    })
                    break  # Count child once even if both parents incarcerated

        return {
            "count": len(children_with_incarcerated),
            "total_children": total_children,
            "details": children_with_incarcerated,
            "incarcerated_parent_count": len(incarcerated_nodes),
        }

    def count_incarcerated_with_foster_children(self) -> dict[str, Any]:
        """
        Bidirectional query: How many incarcerated individuals
        have children in foster care?
        """
        incarcerated_nodes = {
            e.source_id
            for e in self.graph.get_all_of_type("INCARCERATED")
        }

        # Find parents who are both connected to children AND incarcerated
        incarcerated_parents = set()
        parent_to_children: dict[str, list[str]] = defaultdict(list)

        for node_id, attrs in self.graph.nodes.items():
            if attrs.get("type") != "child":
                continue
            parent_edges = (
                self.graph.get_neighbors(node_id, "HAS_MOTHER") +
                self.graph.get_neighbors(node_id, "HAS_FATHER")
            )
            for edge in parent_edges:
                if edge.target_id in incarcerated_nodes:
                    incarcerated_parents.add(edge.target_id)
                    parent_to_children[edge.target_id].append(node_id)

        return {
            "count": len(incarcerated_parents),
            "total_incarcerated": len(incarcerated_nodes),
            "details": [
                {"parent_id": pid, "children": cids}
                for pid, cids in parent_to_children.items()
            ],
        }

    def count_foster_youth_with_juvenile_record(self) -> dict[str, Any]:
        """
        Cross-agency: How many foster children also have juvenile
        detention records?
        """
        juvenile_nodes = {
            e.source_id
            for e in self.graph.get_all_of_type("JUVENILE_RECORD")
        }

        foster_with_juvenile = []
        total_children = 0

        for node_id, attrs in self.graph.nodes.items():
            if attrs.get("type") != "child":
                continue
            total_children += 1
            if node_id in juvenile_nodes:
                foster_with_juvenile.append(node_id)

        return {
            "count": len(foster_with_juvenile),
            "total_children": total_children,
            "ids": foster_with_juvenile,
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

def reasoning_node(state: InsightState) -> InsightState:
    """
    LangGraph node: Cross-Agency Reasoning.

    Builds ephemeral graph from agency data in state,
    then computes the cross-agency answer.
    """
    reasoner = CrossAgencyReasoner()

    idhw_data = state.get("idhw_data", {})
    idoc_data = state.get("idoc_data", {})
    idjc_data = state.get("idjc_data", {})

    # Build graph from available agency data
    if idhw_data:
        try:
            reasoner.build_family_graph(idhw_data)
        except Exception as e:
            state.setdefault("errors", []).append(f"IDHW graph build error: {e}")
            logger.error(f"Failed to build IDHW family graph: {e}")

    if idoc_data:
        try:
            reasoner.add_incarceration_data(idoc_data)
        except Exception as e:
            state.setdefault("errors", []).append(f"IDOC graph overlay error: {e}")
            logger.error(f"Failed to overlay IDOC data: {e}")

    if idjc_data:
        try:
            reasoner.add_juvenile_data(idjc_data)
        except Exception as e:
            state.setdefault("errors", []).append(f"IDJC graph overlay error: {e}")
            logger.error(f"Failed to overlay IDJC data: {e}")

    # Determine which computation to run based on the plan/intent
    intent = state.get("intent", "")
    question = state.get("question", "").lower()
    plan = state.get("plan", [])

    result: dict[str, Any] = {}

    # Route to the appropriate computation
    if _is_foster_incarceration_query(question, plan):
        result = reasoner.count_children_with_incarcerated_parents()
        result["query_type"] = "foster_children_with_incarcerated_parents"

    elif _is_incarcerated_with_children_query(question, plan):
        result = reasoner.count_incarcerated_with_foster_children()
        result["query_type"] = "incarcerated_with_foster_children"

    elif _is_foster_juvenile_query(question, plan):
        result = reasoner.count_foster_youth_with_juvenile_record()
        result["query_type"] = "foster_youth_with_juvenile_record"

    else:
        # Generic: try all and return whatever has data
        foster_inc = reasoner.count_children_with_incarcerated_parents()
        if foster_inc["count"] > 0:
            result = foster_inc
            result["query_type"] = "foster_children_with_incarcerated_parents"
        else:
            result = {
                "query_type": "unknown",
                "graph_stats": {
                    "nodes": reasoner.graph.node_count,
                    "edges": reasoner.graph.edge_count,
                },
                "note": "Could not determine specific computation from query."
            }

    # Add confidence and graph metadata
    result["confidence"] = reasoner.compute_confidence(result)
    result["graph_stats"] = {
        "nodes": reasoner.graph.node_count,
        "edges": reasoner.graph.edge_count,
    }
    result["timestamp"] = datetime.now(timezone.utc).isoformat()

    state["reasoning_result"] = result
    state["confidence"] = result["confidence"]

    # Update execution trace
    trace = state.get("execution_trace", [])
    trace.append(
        f"[Reasoning] Built ephemeral graph: {reasoner.graph.node_count} nodes, "
        f"{reasoner.graph.edge_count} edges. Result: {result.get('query_type')} "
        f"count={result.get('count', 'N/A')}, confidence={result['confidence']}"
    )
    state["execution_trace"] = trace

    # Build sources list
    sources = state.get("sources", [])
    for edge in reasoner.graph.edges:
        if edge.source_agency not in sources:
            sources.append(edge.source_agency)
    state["sources"] = sources

    logger.info(
        f"Reasoning complete: {result.get('query_type')} "
        f"count={result.get('count', 'N/A')} "
        f"confidence={result['confidence']}"
    )

    return state


# ── Helper functions for query routing ──────────────────────────

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
