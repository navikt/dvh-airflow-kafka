# Builder stage: install dependencies into a virtual environment
FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.13-dev AS builder

ENV PYTHONFAULTHANDLER=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR "/app"

# Install uv via pip, then use it to sync dependencies
COPY pyproject.toml uv.lock ./

RUN pip install uv && \
    uv sync --locked --no-dev

# App stage: minimal runtime image
FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.13

LABEL org.opencontainers.image.source="https://github.com/navikt/dvh-airflow-kafka"

ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR "/app"

# Copy virtual environment from builder and application source
COPY --from=builder "/app/.venv" "/app/.venv"
COPY src/ ./src/

ENTRYPOINT ["/app/.venv/bin/python"]
CMD ["-m", "src.main"]
