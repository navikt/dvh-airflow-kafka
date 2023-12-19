FROM navikt/python:3.11
LABEL org.opencontainers.image.source "https://github.com/navikt/dvh-airflow-kafka"

USER root

WORKDIR /app

COPY poetry.lock pyproject.toml ./

RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --only main

COPY src ./

USER apprunner
