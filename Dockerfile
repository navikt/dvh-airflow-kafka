FROM navikt/python:3.9
LABEL org.opencontainers.image.source "https://github.com/navikt/dvh-airflow-kafka"

USER root

RUN apt-get update && apt-get install -yq --no-install-recommends wget libaio1 alien \
    && wget https://download.oracle.com/otn_software/linux/instantclient/211000/oracle-instantclient-basiclite-21.1.0.0.0-1.x86_64.rpm \
    && alien -i oracle-instantclient-basiclite-21.1.0.0.0-1.x86_64.rpm \
    && rm -f oracle-instantclient-basiclite-21.1.0.0.0-1.x86_64.rpm \
    && rm -rf /var/cache/apt/archives \
    && echo /usr/lib/oracle/21.1/client64/lib > /etc/ld.so.conf.d/oracle-instantclient21.1.conf \
    && ldconfig

WORKDIR /app

COPY poetry.lock pyproject.toml ./

RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

COPY src ./

USER apprunner

ENV PATH=$PATH:/usr/lib/oracle/21.1/client64/bin
