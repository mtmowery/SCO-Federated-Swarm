FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY agents/__init__.py ./agents/
COPY agents/idjc/ ./agents/idjc/
COPY prompts/idjc_agent.txt ./prompts/

EXPOSE 8002

CMD ["python", "-m", "agents.idjc.mcp_server"]
