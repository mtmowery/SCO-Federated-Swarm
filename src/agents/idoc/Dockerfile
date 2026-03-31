FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY agents/__init__.py ./agents/
COPY agents/idoc/ ./agents/idoc/
COPY prompts/idoc_agent.txt ./prompts/

EXPOSE 8003

CMD ["python", "-m", "agents.idoc.mcp_server"]
