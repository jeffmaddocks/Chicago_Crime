FROM apache/superset:latest

USER root
RUN /app/.venv/bin/python -m ensurepip \
    && /app/.venv/bin/python -m pip install --no-cache-dir duckdb duckdb-engine pyarrow
USER superset
