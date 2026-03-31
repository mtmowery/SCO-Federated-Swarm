"""
Neo4j Graph Memory

Persistent relationship graph that materializes cross-agency
relationships for fast repeated queries. This is the long-term
intelligence store that grows over time.

Phase 2: Load person nodes and family edges from IDHW
Phase 3: Add IDOC/IDJC offense and sentence subgraphs
Phase 5+: Agents query Neo4j directly for materialized relationships
"""

from __future__ import annotations

import logging
from typing import Any

from neo4j import AsyncGraphDatabase, AsyncDriver

from shared.config import get_settings

logger = logging.getLogger(__name__)


class GraphMemory:
    """
    Neo4j-backed persistent graph memory.

    Stores materialized cross-agency relationships so that
    frequently-asked questions can be answered with a single
    Cypher query instead of multi-step MCP orchestration.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._driver: AsyncDriver | None = None
        self._uri = settings.neo4j.bolt_uri
        self._user = settings.neo4j.user
        self._password = settings.neo4j.password
        self._database = "neo4j"  # Community edition default

    async def connect(self) -> None:
        """Initialize Neo4j driver."""
        self._driver = AsyncGraphDatabase.driver(
            self._uri,
            auth=(self._user, self._password),
        )
        # Verify connectivity
        async with self._driver.session(database=self._database) as session:
            result = await session.run("RETURN 1 AS ping")
            await result.single()
        logger.info(f"Neo4j connected: {self._uri}/{self._database}")

    async def close(self) -> None:
        """Close Neo4j driver."""
        if self._driver:
            await self._driver.close()
            self._driver = None

    @property
    def driver(self) -> AsyncDriver:
        if self._driver is None:
            raise RuntimeError("Neo4j not connected. Call connect() first.")
        return self._driver

    # ── Schema Setup ────────────────────────────────────────────

    async def create_constraints_and_indexes(self) -> None:
        """Create Neo4j constraints and indexes for the graph model."""
        async with self.driver.session(database=self._database) as session:
            constraints = [
                "CREATE CONSTRAINT person_insight_id IF NOT EXISTS "
                "FOR (p:Person) REQUIRE p.insight_id IS UNIQUE",

                "CREATE INDEX person_agency IF NOT EXISTS "
                "FOR (p:Person) ON (p.agency_source)",

                "CREATE INDEX person_type IF NOT EXISTS "
                "FOR (p:Person) ON (p.person_type)",
            ]
            for cypher in constraints:
                try:
                    await session.run(cypher)
                except Exception as e:
                    logger.warning(f"Constraint/index creation note: {e}")

            logger.info("Neo4j constraints and indexes created")

    # ── Data Loading ────────────────────────────────────────────

    async def load_idhw_persons(self, records: list[dict]) -> int:
        """
        Load IDHW person nodes and family relationships.

        Creates :Person nodes with :Child label for children,
        and HAS_MOTHER / HAS_FATHER relationships.
        """
        count = 0
        async with self.driver.session(database=self._database) as session:
            for record in records:
                insight_id = record.get("insight_id")
                if not insight_id:
                    continue

                person_type = record.get("person_type", "unknown")
                labels = ":Person:Child" if person_type == "child" else ":Person"

                # Upsert person node
                await session.run(
                    f"MERGE (p{labels} {{insight_id: $insight_id}}) "
                    "SET p.first_name = $first_name, "
                    "    p.last_name = $last_name, "
                    "    p.dob = $dob, "
                    "    p.gender = $gender, "
                    "    p.person_type = $person_type, "
                    "    p.agency_source = 'IDHW', "
                    "    p.start_care_date = $start_care_date, "
                    "    p.end_care_date = $end_care_date, "
                    "    p.end_reason = $end_reason",
                    insight_id=insight_id,
                    first_name=record.get("first_name"),
                    last_name=record.get("last_name"),
                    dob=record.get("dob"),
                    gender=record.get("gender"),
                    person_type=person_type,
                    start_care_date=record.get("start_care_date"),
                    end_care_date=record.get("end_care_date"),
                    end_reason=record.get("end_reason"),
                )

                # Create mother relationship
                mother_id = record.get("mother_insight_id")
                if mother_id:
                    await session.run(
                        "MERGE (c:Person {insight_id: $child_id}) "
                        "MERGE (m:Person {insight_id: $mother_id}) "
                        "SET m.person_type = COALESCE(m.person_type, 'mother') "
                        "MERGE (c)-[:HAS_MOTHER]->(m)",
                        child_id=insight_id,
                        mother_id=mother_id,
                    )

                # Create father relationship
                father_id = record.get("father_insight_id")
                if father_id:
                    await session.run(
                        "MERGE (c:Person {insight_id: $child_id}) "
                        "MERGE (f:Person {insight_id: $father_id}) "
                        "SET f.person_type = COALESCE(f.person_type, 'father') "
                        "MERGE (c)-[:HAS_FATHER]->(f)",
                        child_id=insight_id,
                        father_id=father_id,
                    )

                count += 1

        logger.info(f"Loaded {count} IDHW persons into Neo4j")
        return count

    async def load_idoc_sentences(self, records: list[dict]) -> int:
        """
        Load IDOC sentence data. Creates :Sentence nodes
        and SERVING_SENTENCE relationships.
        """
        count = 0
        async with self.driver.session(database=self._database) as session:
            for record in records:
                insight_id = record.get("insight_id")
                if not insight_id:
                    continue

                # Upsert person node
                await session.run(
                    "MERGE (p:Person {insight_id: $insight_id}) "
                    "SET p.agency_source = CASE WHEN p.agency_source IS NULL "
                    "    THEN 'IDOC' ELSE p.agency_source + ',IDOC' END",
                    insight_id=insight_id,
                )

                # Create sentence node
                await session.run(
                    "MERGE (p:Person {insight_id: $insight_id}) "
                    "CREATE (s:Sentence {"
                    "  offense_desc: $offense_desc, "
                    "  crime_group: $crime_group, "
                    "  sent_status: $sent_status, "
                    "  mitt_status: $mitt_status, "
                    "  sent_beg_dtd: $sent_beg_dtd, "
                    "  sent_ft_dtd: $sent_ft_dtd"
                    "}) "
                    "CREATE (p)-[:SERVING_SENTENCE]->(s)",
                    insight_id=insight_id,
                    offense_desc=record.get("off_ldesc", record.get("offense_desc")),
                    crime_group=record.get("crm_grp_desc", record.get("crime_group")),
                    sent_status=record.get("sent_status"),
                    mitt_status=record.get("mitt_status"),
                    sent_beg_dtd=record.get("sent_beg_dtd"),
                    sent_ft_dtd=record.get("sent_ft_dtd"),
                )
                count += 1

        logger.info(f"Loaded {count} IDOC sentences into Neo4j")
        return count

    async def load_idjc_commitments(self, records: list[dict]) -> int:
        """
        Load IDJC commitment data. Creates :Commitment nodes
        and JUVENILE_COMMITMENT relationships.
        """
        count = 0
        async with self.driver.session(database=self._database) as session:
            for record in records:
                insight_id = record.get("insight_id")
                if not insight_id:
                    continue

                await session.run(
                    "MERGE (p:Person {insight_id: $insight_id}) "
                    "SET p.agency_source = CASE WHEN p.agency_source IS NULL "
                    "    THEN 'IDJC' ELSE p.agency_source + ',IDJC' END",
                    insight_id=insight_id,
                )

                await session.run(
                    "MERGE (p:Person {insight_id: $insight_id}) "
                    "CREATE (c:Commitment {"
                    "  offense_desc: $offense_desc, "
                    "  offense_category: $offense_category, "
                    "  offense_level: $offense_level, "
                    "  status: $status, "
                    "  date_of_commitment: $commitment_date, "
                    "  date_of_release: $release_date, "
                    "  county: $county"
                    "}) "
                    "CREATE (p)-[:JUVENILE_COMMITMENT]->(c)",
                    insight_id=insight_id,
                    offense_desc=record.get("OFFENSE_DESCRIPTION", record.get("offense_description")),
                    offense_category=record.get("OFFENSE_CATEGORY", record.get("offense_category")),
                    offense_level=record.get("OFFENSE_LEVEL", record.get("offense_level")),
                    status=record.get("STATUS", record.get("status")),
                    commitment_date=record.get("DATE_OF_COMMITMENT", record.get("date_of_commitment")),
                    release_date=record.get("DATE_OF_RELEASE", record.get("date_of_release")),
                    county=record.get("COMMITTING_COUNTY", record.get("committing_county")),
                )
                count += 1

        logger.info(f"Loaded {count} IDJC commitments into Neo4j")
        return count

    # ── Graph Queries ───────────────────────────────────────────

    async def count_foster_children_with_incarcerated_parents(self) -> dict:
        """
        Core cross-agency Cypher query:
        How many foster children have at least one parent with
        an active sentence?
        """
        async with self.driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH (c:Child)-[:HAS_MOTHER|HAS_FATHER]->(p:Person)"
                "-[:SERVING_SENTENCE]->(s:Sentence) "
                "WHERE s.sent_status <> 'DISCHARGED' "
                "RETURN count(DISTINCT c) AS count, "
                "       count(DISTINCT p) AS parent_count"
            )
            record = await result.single()
            return {
                "foster_children_with_incarcerated_parents": record["count"] if record else 0,
                "incarcerated_parents": record["parent_count"] if record else 0,
                "source": "neo4j_materialized",
            }

    async def count_incarcerated_with_foster_children(self) -> dict:
        """Bidirectional: incarcerated individuals who have foster children."""
        async with self.driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH (p:Person)-[:SERVING_SENTENCE]->(s:Sentence), "
                "      (c:Child)-[:HAS_MOTHER|HAS_FATHER]->(p) "
                "WHERE s.sent_status <> 'DISCHARGED' "
                "RETURN count(DISTINCT p) AS count"
            )
            record = await result.single()
            return {
                "incarcerated_with_foster_children": record["count"] if record else 0,
                "source": "neo4j_materialized",
            }

    async def count_foster_youth_with_juvenile_record(self) -> dict:
        """Foster children who also have juvenile commitment records."""
        async with self.driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH (c:Child)-[:JUVENILE_COMMITMENT]->(commit:Commitment) "
                "RETURN count(DISTINCT c) AS count"
            )
            record = await result.single()
            return {
                "foster_youth_with_juvenile_record": record["count"] if record else 0,
                "source": "neo4j_materialized",
            }

    async def get_family_network(self, insight_id: str, depth: int = 2) -> dict:
        """Get the family network for a person up to N hops."""
        async with self.driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH path = (p:Person {insight_id: $insight_id})"
                "-[*1.." + str(depth) + "]-(connected) "
                "RETURN nodes(path) AS nodes, relationships(path) AS rels "
                "LIMIT 100",
                insight_id=insight_id,
            )
            records = [r async for r in result]
            nodes = set()
            edges = []
            for record in records:
                for node in record["nodes"]:
                    nodes.add(node.get("insight_id", str(node.id)))
                for rel in record["rels"]:
                    edges.append({
                        "type": rel.type,
                        "start": rel.start_node.get("insight_id", ""),
                        "end": rel.end_node.get("insight_id", ""),
                    })
            return {
                "root": insight_id,
                "nodes": list(nodes),
                "edges": edges,
                "depth": depth,
            }

    async def get_graph_stats(self) -> dict:
        """Get graph statistics."""
        async with self.driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH (n) RETURN count(n) AS nodes "
                "UNION ALL "
                "MATCH ()-[r]->() RETURN count(r) AS nodes"
            )
            records = [r async for r in result]
            node_count = records[0]["nodes"] if records else 0
            edge_count = records[1]["nodes"] if len(records) > 1 else 0
            return {
                "nodes": node_count,
                "edges": edge_count,
            }


# Module-level singleton
_graph_instance: GraphMemory | None = None


async def get_graph_memory() -> GraphMemory:
    """Get or create the graph memory singleton."""
    global _graph_instance
    if _graph_instance is None:
        _graph_instance = GraphMemory()
        await _graph_instance.connect()
        await _graph_instance.create_constraints_and_indexes()
    return _graph_instance
