"""
Idaho Federated AI Swarm — Main Entry Point

CLI runner for the federated intelligence mesh. Provides commands for:
  - Running the controller API server
  - Loading CSV data into PostgreSQL
  - Populating the Neo4j knowledge graph
  - Running ad-hoc queries against the swarm
  - Health-checking all services

Usage:
    python main.py serve          # Start the controller API
    python main.py load-csv       # Load CSVs into PostgreSQL
    python main.py load-graph     # Populate Neo4j from PostgreSQL
    python main.py load-all       # Load CSV + graph in sequence
    python main.py query "..."    # Run a one-shot query
    python main.py health         # Check all service health
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time

import uvicorn

from shared.config import settings
from shared.logging_config import setup_logging

logger = logging.getLogger(__name__)


# ── Controller API Server ────────────────────────────────────────────────────

def cmd_serve(args: argparse.Namespace) -> None:
    """Start the controller FastAPI server."""
    from controller.api import create_app

    app = create_app()
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="info",
        reload=args.reload,
    )


# ── Data Loading ─────────────────────────────────────────────────────────────

async def _load_csv(data_dir: str | None, agency: str) -> None:
    from data.loaders.csv_loader import CSVLoader

    loader = CSVLoader(data_dir=data_dir)
    if agency == "all":
        await loader.load_all()
    elif agency == "idhw":
        await loader.load_idhw()
    elif agency == "idjc":
        await loader.load_idjc()
    elif agency == "idoc":
        await loader.load_idoc()
    else:
        logger.error(f"Unknown agency: {agency}")
        sys.exit(1)


async def _load_graph() -> None:
    from data.loaders.graph_loader import GraphLoader

    loader = GraphLoader()
    await loader.load_all()


def cmd_load_csv(args: argparse.Namespace) -> None:
    """Load agency CSV files into PostgreSQL."""
    asyncio.run(_load_csv(args.data_dir, args.agency))


def cmd_load_graph(args: argparse.Namespace) -> None:
    """Populate Neo4j knowledge graph from PostgreSQL data."""
    asyncio.run(_load_graph())


def cmd_load_all(args: argparse.Namespace) -> None:
    """Load CSV data then populate graph — full pipeline."""

    async def _pipeline():
        logger.info("Phase 1: Loading CSV data into PostgreSQL...")
        from data.loaders.csv_loader import CSVLoader
        csv_loader = CSVLoader(data_dir=args.data_dir)
        await csv_loader.load_all()

        logger.info("Phase 2: Populating Neo4j from PostgreSQL...")
        from data.loaders.graph_loader import GraphLoader
        graph_loader = GraphLoader()
        await graph_loader.load_all()

        logger.info("Data pipeline complete.")

    asyncio.run(_pipeline())


# ── Ad-hoc Query ─────────────────────────────────────────────────────────────

async def _run_query(question: str) -> dict:
    from controller.graph import run_query
    from security.audit import AuditLogger

    audit = AuditLogger()
    start = time.monotonic()

    result = await run_query(question)

    elapsed_ms = int((time.monotonic() - start) * 1000)
    request_id = await audit.log_cross_agency_query(
        question=question,
        result=result,
        agencies=result.get("sources", []),
        execution_time_ms=elapsed_ms,
    )
    await audit.close()

    result["request_id"] = request_id
    result["execution_time_ms"] = elapsed_ms
    return result


def cmd_query(args: argparse.Namespace) -> None:
    """Run a single query through the swarm."""
    result = asyncio.run(_run_query(args.question))
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"\nAnswer: {result.get('answer', 'No answer')}")
        print(f"Confidence: {result.get('confidence', 0):.1%}")
        if result.get("errors"):
            print(f"Errors: {result['errors']}")
        print(f"Request ID: {result.get('request_id', 'N/A')}")
        print(f"Execution time: {result.get('execution_time_ms', 0)}ms")


# ── Health Check ─────────────────────────────────────────────────────────────

async def _health_check() -> dict:
    import httpx

    services = {
        "controller": f"http://localhost:8000/health",
        "idhw_agent": f"http://{settings.mcp.idhw_host}:{settings.mcp.idhw_port}/health",
        "idjc_agent": f"http://{settings.mcp.idjc_host}:{settings.mcp.idjc_port}/health",
        "idoc_agent": f"http://{settings.mcp.idoc_host}:{settings.mcp.idoc_port}/health",
    }

    results = {}
    async with httpx.AsyncClient(timeout=5.0) as client:
        for name, url in services.items():
            try:
                resp = await client.get(url)
                results[name] = {
                    "status": "healthy" if resp.status_code == 200 else "unhealthy",
                    "code": resp.status_code,
                }
            except Exception as e:
                results[name] = {"status": "unreachable", "error": str(e)}

    return results


def cmd_health(args: argparse.Namespace) -> None:
    """Check health of all services."""
    results = asyncio.run(_health_check())
    all_healthy = all(r["status"] == "healthy" for r in results.values())

    for name, status in results.items():
        icon = "OK" if status["status"] == "healthy" else "FAIL"
        print(f"  [{icon}] {name}: {status['status']}")

    sys.exit(0 if all_healthy else 1)


# ── Controller API App Factory ───────────────────────────────────────────────

def _ensure_controller_api():
    """Create controller/api.py if it doesn't exist (convenience)."""
    import os
    api_path = os.path.join(os.path.dirname(__file__), "controller", "api.py")
    if not os.path.exists(api_path):
        _create_controller_api(api_path)


def _create_controller_api(path: str) -> None:
    """Generate the controller FastAPI app module."""
    # This is handled by the separate controller/api.py file
    pass


# ── CLI Argument Parser ──────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="idaho-swarm",
        description="Idaho Federated AI Swarm — Cross-Agency Intelligence Mesh",
    )
    sub = parser.add_subparsers(dest="command", help="Available commands")

    # serve
    p_serve = sub.add_parser("serve", help="Start the controller API server")
    p_serve.add_argument("--host", default="0.0.0.0", help="Bind host")
    p_serve.add_argument("--port", type=int, default=8000, help="Bind port")
    p_serve.add_argument("--reload", action="store_true", help="Enable auto-reload")
    p_serve.set_defaults(func=cmd_serve)

    # load-csv
    p_csv = sub.add_parser("load-csv", help="Load CSV files into PostgreSQL")
    p_csv.add_argument("--data-dir", default=None, help="Path to CSV data directory")
    p_csv.add_argument("--agency", default="all", choices=["all", "idhw", "idjc", "idoc"])
    p_csv.set_defaults(func=cmd_load_csv)

    # load-graph
    p_graph = sub.add_parser("load-graph", help="Populate Neo4j from PostgreSQL")
    p_graph.set_defaults(func=cmd_load_graph)

    # load-all
    p_all = sub.add_parser("load-all", help="Full pipeline: CSV -> PostgreSQL -> Neo4j")
    p_all.add_argument("--data-dir", default=None, help="Path to CSV data directory")
    p_all.set_defaults(func=cmd_load_all)

    # query
    p_query = sub.add_parser("query", help="Run a query against the swarm")
    p_query.add_argument("question", help="Natural language question")
    p_query.add_argument("--json", action="store_true", help="Output as JSON")
    p_query.set_defaults(func=cmd_query)

    # health
    p_health = sub.add_parser("health", help="Check all service health")
    p_health.set_defaults(func=cmd_health)

    return parser


def main() -> None:
    setup_logging()
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
