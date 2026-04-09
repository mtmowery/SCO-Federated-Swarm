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

    # ── Schema Setup ────────────────────────────────────────────
    async def create_constraints_and_indexes(self) -> None:
        """Create Neo4j constraints and indexes for the refined minimalist model."""
        async with self.driver.session(database=self._database) as session:
            setup_commands = [
                # Unique constraint on Person insight_id
                "CREATE CONSTRAINT person_insight_id IF NOT EXISTS "
                "FOR (p:Person) REQUIRE p.insight_id IS UNIQUE",

                # Unique constraint on Agency agency_id
                "CREATE CONSTRAINT agency_id_unique IF NOT EXISTS "
                "FOR (a:Agency) REQUIRE a.agency_id IS UNIQUE",

                # Unique constraint on County name
                "CREATE CONSTRAINT county_name_unique IF NOT EXISTS "
                "FOR (c:County) REQUIRE c.name IS UNIQUE",

                # Indexes for faster trait-based matching if needed
                "CREATE INDEX person_traits IF NOT EXISTS "
                "FOR (p:Person) ON (p.dob_year, p.dob_month, p.gender)",
            ]
            for cypher in setup_commands:
                try:
                    await session.run(cypher)
                except Exception as e:
                    logger.warning(f"Constraint/index creation note: {e}")

            logger.info("Neo4j constraints and indexes created")

    # ── Data Loading ────────────────────────────────────────────

    def _extract_active_years(self, start_dt: str | None, end_dt: str | None) -> list[int]:
        """Extract a list of active years between two date strings."""
        import re
        years = set()
        for dt in (start_dt, end_dt):
            if dt:
                match = re.search(r'\b(19|20)\d{2}\b', str(dt))
                if match:
                    years.add(int(match.group(0)))
        if len(years) == 2:
            return list(range(min(years), max(years) + 1))
        return list(years)

    async def _ensure_agency(self, session, agency_id: str) -> None:
        """Ensure an Agency node exists."""
        await session.run(
            "MERGE (a:Agency {agency_id: $agency_id})",
            agency_id=agency_id
        )

    async def load_idhw_persons(self, records: list[dict]) -> int:
        """
        Load IDHW person nodes and family relationships.
        Refined Schema: Person(insight_id, dob_year, dob_month, gender), Agency(IDHW), IN_AGENCY, PARENT_OF.
        """
        count = 0
        async with self.driver.session(database=self._database) as session:
            # Ensure IDHW Agency node
            await self._ensure_agency(session, "IDHW")

            for record in records:
                insight_id = record.get("insight_id")
                if not insight_id:
                    continue

                # Upsert minimalist person node
                await session.run(
                    "MERGE (p:Person {insight_id: $insight_id}) "
                    "SET p.dob_month = $dob_month, "
                    "    p.dob_year = $dob_year, "
                    "    p.gender = $gender, "
                    "    p.death_date = $death_date",
                    insight_id=insight_id,
                    dob_month=record.get("dob_month"),
                    dob_year=record.get("dob_year"),
                    gender=record.get("gender"),
                    death_date=record.get("death_date")
                )

                # Link to IDHW Agency
                active_years = self._extract_active_years(record.get("start_care_date"), record.get("end_care_date"))
                await session.run(
                    "MATCH (p:Person {insight_id: $insight_id}), (a:Agency {agency_id: 'IDHW'}) "
                    "MERGE (a)-[r:IN_AGENCY]->(p) "
                    "SET r.active_years = $active_years",
                    insight_id=insight_id,
                    active_years=active_years
                )

                # Create PARENT_OF relationships (from parents TO child)
                child_id = record.get("child_insight_id") or insight_id
                mother_id = record.get("mother_insight_id")
                father_id = record.get("father_insight_id")

                if mother_id:
                    await session.run(
                        "MERGE (c:Person {insight_id: $child_id}) "
                        "MERGE (m:Person {insight_id: $mother_id}) "
                        "MERGE (m)-[:PARENT_OF {role: 'Mother'}]->(c)",
                        child_id=child_id,
                        mother_id=mother_id,
                    )

                if father_id:
                    await session.run(
                        "MERGE (c:Person {insight_id: $child_id}) "
                        "MERGE (f:Person {insight_id: $father_id}) "
                        "MERGE (f)-[:PARENT_OF {role: 'Father'}]->(c)",
                        child_id=child_id,
                        father_id=father_id,
                    )

                count += 1

        logger.info(f"Loaded {count} IDHW records into refined graph")
        return count

    async def load_idoc_sentences(self, records: list[dict]) -> int:
        """
        Load IDOC memberships.
        Minimalist Schema: Person nodes + link to IDOC Agency.
        """
        count = 0
        async with self.driver.session(database=self._database) as session:
            await self._ensure_agency(session, "IDOC")

            for record in records:
                insight_id = record.get("insight_id")
                if not insight_id:
                    continue

                # Upsert person traits (coalesce to preserve existing data)
                await session.run(
                    "MERGE (p:Person {insight_id: $insight_id}) "
                    "SET p.gender = COALESCE(p.gender, $gender), "
                    "    p.dob_month = COALESCE(p.dob_month, $dob_month), "
                    "    p.dob_year = COALESCE(p.dob_year, $dob_year)",
                    insight_id=insight_id,
                    gender=record.get("gender"),
                    dob_month=record.get("dob_month"),
                    dob_year=record.get("dob_year"),
                )

                # Link to IDOC Agency
                active_years = self._extract_active_years(record.get("sent_beg_dtd"), record.get("sent_ft_dtd"))
                await session.run(
                    "MATCH (p:Person {insight_id: $insight_id}), (a:Agency {agency_id: 'IDOC'}) "
                    "MERGE (a)-[r:IN_AGENCY]->(p) "
                    "SET r.active_years = $active_years",
                    insight_id=insight_id,
                    active_years=active_years
                )

                # Link to County
                county = record.get("cnty_sdesc")
                if county and str(county).strip():
                    await session.run(
                        "MERGE (c:County {name: $county}) "
                        "MERGE (p:Person {insight_id: $insight_id}) "
                        "MERGE (p)-[:ASSOCIATED_WITH]->(c)",
                        county=str(county).title().strip(),
                        insight_id=insight_id
                    )
                count += 1

        logger.info(f"Loaded {count} IDOC memberships into refined graph")
        return count

    async def load_idjc_commitments(self, records: list[dict]) -> int:
        """
        Load IDJC memberships.
        Minimalist Schema: Person nodes + link to IDJC Agency.
        """
        count = 0
        async with self.driver.session(database=self._database) as session:
            await self._ensure_agency(session, "IDJC")

            for record in records:
                insight_id = record.get("insight_id")
                if not insight_id:
                    continue

                await session.run(
                    "MERGE (p:Person {insight_id: $insight_id}) "
                    "SET p.gender = COALESCE(p.gender, $gender), "
                    "    p.dob_month = COALESCE(p.dob_month, $dob_month), "
                    "    p.dob_year = COALESCE(p.dob_year, $dob_year)",
                    insight_id=insight_id,
                    gender=record.get("gender"),
                    dob_month=record.get("dob_month"),
                    dob_year=record.get("dob_year"),
                )

                # Link to IDJC Agency
                active_years = self._extract_active_years(record.get("date_of_commitment"), record.get("date_of_release"))
                await session.run(
                    "MATCH (p:Person {insight_id: $insight_id}), (a:Agency {agency_id: 'IDJC'}) "
                    "MERGE (a)-[r:IN_AGENCY]->(p) "
                    "SET r.active_years = $active_years",
                    insight_id=insight_id,
                    active_years=active_years
                )

                # Link to County
                county = record.get("committing_county")
                if county and str(county).strip():
                    await session.run(
                        "MERGE (c:County {name: $county}) "
                        "MERGE (p:Person {insight_id: $insight_id}) "
                        "MERGE (p)-[:ASSOCIATED_WITH]->(c)",
                        county=str(county).title().strip(),
                        insight_id=insight_id
                    )
                count += 1

        logger.info(f"Loaded {count} IDJC memberships into refined graph")
        return count

    # ── Refined Graph Queries ───────────────────────────────────

    async def count_foster_children_with_incarcerated_parents(self) -> dict:
        """
        Cross-agency query using minimalist schema:
        Person in IDHW (child) is linked via PARENT_OF to Person in IDOC.
        """
        async with self.driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH (a_idhw:Agency {agency_id: 'IDHW'})-[:IN_AGENCY]->(child:Person) "
                "MATCH (parent:Person)-[:PARENT_OF]->(child) "
                "MATCH (a_idoc:Agency {agency_id: 'IDOC'})-[:IN_AGENCY]->(parent) "
                "RETURN count(DISTINCT child) AS count, "
                "       count(DISTINCT parent) AS parent_count"
            )
            record = await result.single()
            return {
                "foster_children_with_incarcerated_parents": record["count"] if record else 0,
                "incarcerated_parents": record["parent_count"] if record else 0,
                "source": "neo4j_materialized",
            }

    async def count_incarcerated_with_foster_children(self) -> dict:
        """Bidirectional query using minimalist schema."""
        async with self.driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH (a_idoc:Agency {agency_id: 'IDOC'})-[:IN_AGENCY]->(parent:Person) "
                "MATCH (parent)-[:PARENT_OF]->(child:Person) "
                "MATCH (a_idhw:Agency {agency_id: 'IDHW'})-[:IN_AGENCY]->(child) "
                "RETURN count(DISTINCT parent) AS count"
            )
            record = await result.single()
            return {
                "incarcerated_with_foster_children": record["count"] if record else 0,
                "source": "neo4j_materialized",
            }

    async def count_foster_youth_with_juvenile_record(self) -> dict:
        """Symmetry query: Person linked to both IDHW and IDJC."""
        async with self.driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH (a_idhw:Agency {agency_id: 'IDHW'})-[:IN_AGENCY]->(p:Person)<-[:IN_AGENCY]-(a_idjc:Agency {agency_id: 'IDJC'}) "
                "RETURN count(DISTINCT p) AS count"
            )
            record = await result.single()
            return {
                "foster_youth_with_juvenile_record": record["count"] if record else 0,
                "source": "neo4j_materialized",
            }

    async def get_family_network(self, insight_id: str, depth: int = 2) -> dict:
        """Get network around a Person using PARENT_OF."""
        async with self.driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH path = (p:Person {insight_id: $insight_id})"
                "-[*1.." + str(depth) + "]-(connected:Person) "
                "RETURN nodes(path) AS nodes, relationships(path) AS rels "
                "LIMIT 100",
                insight_id=insight_id,
            )
            records = [r async for r in result]
            nodes_data = {}
            edges = []
            for record in records:
                for node in record["nodes"]:
                    nid = node.get("insight_id")
                    if nid and nid not in nodes_data:
                        nodes_data[nid] = {
                            "insight_id": nid,
                            "dob_year": node.get("dob_year"),
                            "gender": node.get("gender")
                        }
                for rel in record["rels"]:
                    edges.append({
                        "type": rel.type,
                        "start": rel.start_node.get("insight_id"),
                        "end": rel.end_node.get("insight_id"),
                    })
            return {
                "root": insight_id,
                "nodes": list(nodes_data.values()),
                "edges": edges,
                "depth": depth,
            }

    async def get_graph_stats(self) -> dict:
        """Get graph statistics for the minimalist model."""
        async with self.driver.session(database=self._database) as session:
            result = await session.run(
                "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count"
            )
            records = [r async for r in result]
            stats = {record["label"]: record["count"] for record in records}

            result_edges = await session.run(
                "MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS count"
            )
            edge_records = [r async for r in result_edges]
            for r in edge_records:
                stats[f"rel_{r['type']}"] = r["count"]

            return stats


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
