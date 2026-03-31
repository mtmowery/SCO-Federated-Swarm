FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source — controller needs shared, reasoning, memory, security
COPY shared/ ./shared/
COPY controller/ ./controller/
COPY reasoning/ ./reasoning/
COPY memory/ ./memory/
COPY security/ ./security/
COPY agents/__init__.py ./agents/
COPY prompts/ ./prompts/
COPY main.py .

EXPOSE 8000

CMD ["python", "main.py", "serve"]
