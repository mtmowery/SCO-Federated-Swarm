# Idaho Federated AI Swarm — Startup Guide

This guide details how to initialize the federated intelligence mesh, start all background infrastructure, and reset the environment to a clean state.

---

## 1. System Requirements

Before starting, ensure you have the following installed:
- **Docker & Docker Compose**: For running the database stack and agency agents.
- **Python 3.10+**: For running the main CLI and data loaders.
- **Ollama**: Must be running standalone on your host machine (default: `localhost:11434`).
  - Mandatory models: `llama3:8b`, `nomic-embed-text`.

---

## 2. Initial Setup

### Step A: Clone & Environment
Navigate to the project root and move into the `src/` directory where the core logic resides.

```bash
cd src
cp .env.example .env
```

### Step B: Virtual Environment
Create a Python virtual environment and install dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 3. Starting the Project

The swarm uses a multi-container Docker stack for infrastructure (PostgreSQL, Neo4j, Qdrant, Redis) and the microservice agents (IDHW, IDJC, IDOC, Controller).

### Start Infrastructure
From the `src/` directory, run:

```bash
docker compose up -d
```

Validating the startup:
- **PostgreSQL**: Port `5434`
- **Neo4j**: Port `7475` (Browser UI), `7688` (Bolt)
- **Qdrant**: Port `6335`
- **Redis**: Port `6381`
- **Controller API**: Port `8000`
- **Web UI**: Port `5056`

---

## 4. Starting the Web UI (Optional)

The project includes a Flask-based chat interface for interacting with the swarm.

```bash
# From the src/ directory
python3 web/app.py
```

Access the UI at: `http://localhost:5056`

---

## 5. Loading Data & Knowledge Graph

The databases start empty. You must run the ingestion pipeline to move data from local CSVs into PostgreSQL and then synchronize the Neo4j knowledge graph.

### Full Pipeline Run
```bash
python main.py load-all
```

*This command will:*
1. Parse CSVs from `data/input_sample_data/`.
2. Populate the agency-specific PostgreSQL databases.
3. Build the cross-agency graph relationships in Neo4j.

---

## 5. Verification & Health Checks

Confirm the swarm is functional and all agents are reachable:

```bash
python main.py health
```

To test a cross-agency query:
```bash
python main.py query "How many children in foster care have parents in the adult corrections system?"
```

---

## 6. Clearing All Databases (Start Fresh)

If you need to nuke the current state and start completely over (e.g., after a schema change or to purge test data), follow these steps:

### The "Nuke" Command
The most effective way to clear the databases is to stop the containers and delete their persistent volumes.

```bash
# 1. Stop all containers and delete volumes (Postgres, Neo4j, etc.)
docker compose down -v

# 2. Restart infrastructure (re-creates clean volumes/databases)
docker compose up -d

# 3. Reload the data pipeline
python3 main.py load-all
```

> [!WARNING]
> Running `docker compose down -v` will permanently delete all data inside the containers including all PostgreSQL records, Neo4j graph nodes, and Qdrant vector collections.

---

## 7. Useful CLI Commands

| Command | Action |
| :--- | :--- |
| `python main.py serve` | Starts the Controller API server (with hot-reload) |
| `python main.py load-all` | Loads CSVs -> PG -> Neo4j |
| `python main.py health` | Checks connectivity to all swarm services |
| `python main.py query "..."` | Runs a natural language query through the mesh |
| `docker compose logs -f` | Follow logs for all services |
