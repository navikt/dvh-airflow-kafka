FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/navikt/dvh-airflow-kafka"

RUN useradd --create-home apprunner

# Copy dependency files
COPY . /app

WORKDIR /app

# Install dependencies in system Python (no virtual environment)
RUN uv sync --locked

USER apprunner

CMD ["uv", "run", "python", "-m", "src.main"]
