FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd -m appuser

COPY pyproject.toml /app/pyproject.toml
COPY chicago_crime /app/chicago_crime
COPY scripts /app/scripts
COPY README.md /app/README.md

RUN pip install --no-cache-dir .

RUN mkdir -p /app/data && chown -R appuser:appuser /app
USER appuser

EXPOSE 8050

CMD ["python", "scripts/run_app.py"]
