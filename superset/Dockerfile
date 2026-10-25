FROM apache/superset:latest

USER root
# ClickHouse driver (SQLAlchemy dialect `clickhousedb://`) into Superset's venv
RUN uv pip install --python /app/.venv/bin/python --no-cache-dir clickhouse-connect
USER superset

# Run migrations + admin + init on every start (all idempotent), then serve.
CMD ["/bin/bash", "-c", "superset db upgrade && (superset fab create-admin --username \"$SUPERSET_ADMIN_USER\" --password \"$SUPERSET_ADMIN_PASSWORD\" --firstname Admin --lastname User --email admin@example.com || true) && superset init && /usr/bin/run-server.sh"]
