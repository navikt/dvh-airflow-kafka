FROM python:3.13-slim
LABEL org.opencontainers.image.source "https://github.com/navikt/dvh-airflow-kafka"

RUN useradd --create-home apprunner

COPY poetry.lock pyproject.toml ./

RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --only main --no-root

WORKDIR /app

COPY . /app

USER apprunner

CMD ["python", "-m", "src.main"]
