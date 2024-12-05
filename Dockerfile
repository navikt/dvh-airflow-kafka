FROM python:3.12-slim
LABEL org.opencontainers.image.source "https://github.com/navikt/dvh-airflow-kafka"

USER root

WORKDIR /app

COPY poetry.lock pyproject.toml ./

RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --only main --no-root

COPY src ./

USER apprunner

CMD ["python", "main.py"]
