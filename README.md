# DVH Airflow Kafka
dvh-airflow-kafka kan benyttes for å konsumere data fra kafka-topics i Aiven og skrive data til tabeller i Oracle on-prem database (DWH). Dette er typisk aktuelt for "dvh"-team som bygger dataprodukter basert på data i "datavarehuset".


Følg denne [guiden](kafka-topic.md) for å opprette en NAIS applikasjon som har tilgang til ønsket topic.

## Bruke kafka-konsument i Airflow
DVH-AIRFLOW-KAFKA bruker `google secret manager` for å laste inn hemligheter for å koble til et kafka topic og oracle. Du angir navnet på hemlighetene slik at konsumenten kan laste inn hemlighetene som miljøvariabler. Som bruker har du disse mulighetene:
1. `SOURCE_SECRET_PATH` & `TARGET_SECRET_PATH`
2. `PROJECT_SECRET_PATH`

Vi anbefaler å legge inn kafka hemligheten i `SOURCE_SECRET_PATH` og oracle hemlighetene i `TARGET_SECRET_PATH`. Alternativt kan de kombineres i en hemliget som angis av `PROJECT_SECRET_PATH`

Dette er miljøvariablene som forventes i SOURCE:
```json
{
  "DB_USER": "user",
  "DB_PASSWORD": "password",
  "DB_DSN": "dsn",
}
```

Dette er miljøvariablene som forventes i TARGET:
```json
{
  "KAFKA_BROKERS": "",
  "KAFKA_CA": "",
  "KAFKA_CERTIFICATE": "",
  "KAFKA_CREDSTORE_PASSWORD": "",
  "KAFKA_PRIVATE_KEY": "",
  "KAFKA_SCHEMA_REGISTRY": "",
  "KAFKA_SCHEMA_REGISTRY_PASSWORD": "",
  "KAFKA_SCHEMA_REGISTRY_USER": "",
  "KAFKA_SECRET_UPDATED": "",
}
```

DVH-AIRFLOW-KAFKA forventer en miljøvariabler `CONSUMER_CONFIG` der verdien er en streng på `yaml` format. Det er denne som bestemmer hvor dataen hentes fra, hvordan den transformeres, og hvor den lagres.\
Kodeeksempel [dv-a-team-dags](https://github.com/navikt/dv-a-team-dags/blob/main/consumer_configs/perm_config.py)

Eksempel config:
```yaml
source:
  type: kafka
  batch-size: 5000
  batch-interval: 5
  topic: topic-navn
  group-id: gruppe-id
  schema: json | avro | string
target:
  type: oracle
  custom-config:
  - method: oracledb.Cursor.setinputsizes
    name: kafka_timestamp
    value: oracledb.TIMESTAMP
  - method: oracledb.Cursor.setinputsizes
    name: kafka_message
    value: oracledb.DB_TYPE_CLOB | oracledb.DB_TYPE_BLOB
  delta:
    column: kafka_timestamp
    table: <target-table-name>
  table: <target-table-name>
  skip-duplicates-with: 
    - kafka_hash
transform:
  - src: kafka_key
    dst: kafka_key
  - src: kafka_offset
    dst: kafka_offset
  - src: kafka_partition
    dst: kafka_partition
  - src: kafka_timestamp
    dst: kafka_timestamp
    fun: int-unix-ms -> datetime-no
  - src: kafka_topic
    dst: kafka_topic
  - src: kafka_hash
    dst: kafka_hash
  - src: kafka_message
    dst: kafka_message
  - src: <kildenavn> # eks $PERMITTERING
    dst: KILDESYSTEM
  - src: $$$BATCH_TIME
    dst: lastet_dato
```


# Development
Intaller [poetry](https://python-poetry.org/docs/)
```bash
poetry install
```

## Kjøre lokalt
```bash
poetry run python src/main.py -l
py src/main -l
```

## kcat

`kcat -b localhost:9092 -t test -C -f '\nKey (%K bytes): %k\t\nValue (%S bytes): %s\nTimestamp: %T\tPartition: %p\tOffset: %o\n--\n'`

## sqlplus

`docker exec -it oracle sqlplus system/example@//localhost:1521/XEPDB1`

`docker exec -it oracle sqlplus kafka/example@//localhost:1521/XEPDB1`

## docker

`d build . -t ghcr.io/navikt/dvh-kafka-airflow-consumer:0.3.`

`d push ghcr.io/navikt/dvh-kafka-airflow-consumer:0.3.`

## cd til Espens utviklingsmiljø prosjektmappe
``cd /mnt/c/Dev/datavarehus/dvh-airflow-kafka/utviklingsmiljo/``

## Stop og slett alle containers
`docker ps -aq | xargs docker stop | xargs docker rm`

## Kjøre opp miljø
`docker-compose up -d`

## Produsere meldinger til test topic
`kafkacat -b localhost:9092 -t test -T -P ./kafka-meldinger/melding-hello.json`

`kafkacat -b localhost:9092 -t test -T -P ./kafka-meldinger/melding-k6.json`

## Sjekke hva som er på topic
`kafkacat -b localhost:9092 -t test`

## Komme inn i zookeeper shell
`docker exec -it $(docker ps -aqf "name=zookeeper") /bin/zookeeper-shell localhost:2181 ls /brokers/topics`

- Liste topics `/bin/zookeeper-shell localhost:2181 ls /brokers/topics`

- Slette alle topics `/bin/zookeeper-shell localhost:2181 deleteall /brokers/topics/test`

## Komme inn i Oracle

`docker exec -it $(docker ps -aqf "name=db") bash`

## Rydde opp i rot
`docker compose down -v`



# Videreutvikling av airflow-konsumenten

Diverse notater og tips for videreutvikling av airflow-konsumenten
