FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared modules and agent code
COPY shared/ ./shared/
COPY agents/__init__.py ./agents/
COPY agents/idhw/ ./agents/idhw/
COPY prompts/idhw_agent.txt ./prompts/

ENV MCP_IDHW_PORT=8000
EXPOSE 8000

CMD ["python", "-m", "agents.idhw.mcp_server"]
