# Airflow image

## Kjøre lokalt

`py src/main -l`

## kcat

`kcat -b localhost:9092 -t test -C -f '\nKey (%K bytes): %k\t\nValue (%S bytes): %s\nTimestamp: %T\tPartition: %p\tOffset: %o\n--\n'`

## sqlplus

`sqlplus system/example@//localhost:1521/XEPDB1`

`sqlplus kafka/example@//localhost:1521/XEPDB1`

## docker

`d build . -t ghcr.io/navikt/dvh-kafka-airflow-consumer:0.3.`

`d push ghcr.io/navikt/dvh-kafka-airflow-consumer:0.3.`
