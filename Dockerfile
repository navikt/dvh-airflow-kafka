FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
LABEL org.opencontainers.image.source="https://github.com/navikt/dvh-airflow-kafka"

USER root
RUN useradd --create-home apprunner

# Copy dependency files
COPY . /app

WORKDIR /app

# Install dependencies in system Python (no virtual environment)
RUN uv sync --locked

ENV PATH="/app/.venv/bin:$PATH"

USER apprunner

CMD ["python", "-m", "src.main"]
