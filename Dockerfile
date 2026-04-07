FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    sqlite3 \
    curl \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && aristotle --version

COPY src/ ./src/
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1
ENV WORKSPACE_ROOT=/data/workspaces
# One-time migration source when upgrading from single shared /data/workspace
ENV WORKSPACE_LEGACY_DIR=/data/workspace
ENV DATABASE_PATH=/data/orchestrator.db
ENV LIMA_DATABASE_PATH=/data/lima.db
ENV PORT=8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD sh -c 'curl -sf "http://127.0.0.1:${PORT}/health" || exit 1'

EXPOSE 8000

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["sh", "-c", "exec uvicorn orchestrator.app:app --host 0.0.0.0 --port ${PORT}"]
