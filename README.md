# DVH Airflow Kafka
dvh-airflow-kafka kan benyttes for å konsumere data fra kafka-topics i Aiven og skrive data til tabeller i Oracle on-prem database (DWH). Dette er typisk aktuelt for "dvh"-team som bygger dataprodukter basert på data i "datavarehuset".


Følg denne [guiden](kafka-topic.md) for å opprette en NAIS applikasjon som har tilgang til ønsket topic.

## Lokal utvikling - kjøre tester

Krever at [Docker desktop](https://www.docker.com/products/docker-desktop/) (Krever lisen) og [Poetry](https://python-poetry.org/docs/) er installert. \
[Podman](https://podman-desktop.io/tutorial/testcontainers-with-podman) er et godt alternativ til Docker Desktop.
- [Podman desktop](https://podman-desktop.io/docs/migrating-from-docker/managing-docker-compatibility)
- [env-vars-fix](https://stackoverflow.com/questions/67642620/docker-credential-desktop-not-installed-or-not-available-in-path)


```bash
poetry run test
poetry run pytest src/development/test_integration.py
poetry run pytest src/development/test_integration.py::test_test_run_subscribe
```


## Bruke kafka-konsument i Airflow
DVH-AIRFLOW-KAFKA bruker `google secret manager` for å laste inn hemligheter for å koble til et kafka topic og oracle. Du angir navnet på hemlighetene slik at konsumenten kan laste inn hemlighetene som miljøvariabler. Som bruker har du disse mulighetene:
1. `SOURCE_SECRET_PATH` & `TARGET_SECRET_PATH`
2. `PROJECT_SECRET_PATH`

Vi anbefaler å legge inn kafka hemligheten i `SOURCE_SECRET_PATH` og oracle hemlighetene i `TARGET_SECRET_PATH`. Alternativt kan de kombineres i en hemliget som angis av `PROJECT_SECRET_PATH`


Dette er miljøvariablene som forventes i SOURCE:
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

Dette er miljøvariablene som forventes i TARGET:
```json
{
  "DB_USER": "user",
  "DB_PASSWORD": "password",
  "DB_DSN": "dsn",
}
```

DVH-AIRFLOW-KAFKA forventer en miljøvariabler `CONSUMER_CONFIG` der verdien er en streng på `yaml` format. Det er denne som bestemmer hvor dataen hentes fra, hvordan den transformeres, og hvor den lagres.\
Kodeeksempel [dv-a-team-dags](https://github.com/navikt/dv-a-team-dags/blob/main/consumer_configs/perm_config.py)

Eksempel config:
```yaml
# Kildekonfigurasjon. 
source:
  # Kun kafka er støttet for øyeblikket
  type: kafka
  # Hvor mange meldinger skal leses før du skriver til target
  batch-size: 5000
  # Minimum tid konsumenten skal polle etter nyemeldinger før den returnerer
  batch-interval: 5
  # Kafka topic som skal kobles til som kilde
  topic: topic-navn
  # Et unikt group id som skal identfirseres som en subscriber på topicet. Hvis du å lese fra start anbefales det å lage en ny group-id, f.eks gruppe-id-v*
  group-id: gruppe-id
  # Type skjema. String er rå streng. Alt aksepteres. JSON skjema har struktur. Avro er streng på datatyper og nullverdier.
  schema: json | avro | string
  # Hvor mange sekunder konsumenten poller før timeout
  poll-timeout: 10 # default 10
  # Velg om du ønsker en konsument som bruker tidsstempel fra bruker til å bestemme offset som skal konsumeres, eller om offset per partisjon comittes til kafka.
  # assign: konsumenten bruker DATA_INTERVAL_START/DATA_INTERVAL_END
  # subscribe: konsumenten comitter offset etter hver batch og fortsetter fra offset som er lagret på topic
  strategy: assign | subscribe # default assign
    # Fjerner feltene key1 og key3 fra json-objektet før det sendes til oracle. Støtter nøstede felter og felter i lister.
  keypath-seperator: /
  message-fields-filter:
    - key1
    - key2/key3
  # Flagger felter med innhold (1 hvis innhold, 0 om ikke) før det sendes til oracle. Støtter nøstede felter og felter i lister.
  flag-field-config:
    - key1
    - key2/key3
  # OBS: Behold KUN de meldingene som matcher regelene nedenfor. Liste med key/value-pairs
  # Alle meldinger som ikke matcher vil bli satt til NONE
  message-filters:
  - key: <feltnavn-kafka-message>
    allowed_value: <verdinavn du vil beholde>
  - key: <nytt eller samme feltnavn-kafka-message>
    allowed_value: <verdinavn du vil beholde>
    ...
    
# Mål
target:
  # Kun Oracle er støttet for øyeblikket
  type: oracle
  custom-config:
  - method: oracledb.Cursor.setinputsizes
    name: kafka_timestamp
    value: oracledb.TIMESTAMP
  - method: oracledb.Cursor.setinputsizes
    name: kafka_message
    value: oracledb.DB_TYPE_CLOB | oracledb.DB_TYPE_BLOB
  # Hvis denne er med vil maksverdi i måltabellen bestemme hvor lesing av kafkatopicet skal starte. Ellers må data_interval_start spesifiseres eksplisitt i DAG.
  delta:
    # Kolonne det skal beregnes maksverdi fra.
    column: kafka_timestamp
    # Stort sett samme som under
    table: <target-table-name>
  # Måltabell
  table: <target-table-name>
  # Hvis du ønsker å filtrere duplikater fra kilde. Tar en liste med en eller flere kolonner.
  skip-duplicates-with: 
    # Hvilke kolonner som til sammen skal være unike.
    - kafka_partition
    - kafka_offset
    - kafka_topic
  # Hvis du ønsker å fjerne meldinger med kode 6/7-skjermede personer. Kafka message settes til null. Støtter nøstede felt.
  k6-filter:
    # Tabell oppslag med fødselsnummer skal gjøres mot, samt kolonne
    filter-table: dt_person.ident_off_id_til_fk_person1
    filter-col: off_id
    timestamp: kafka_timestamp
    col-keypath-separator: /
    # Felt i kafka-meldingen med fødselsnummer
    col: fnr | key1/fnr
# Mapping mellom kildekolonne og målkolonne
transform:
  - src: kafka_key
    dst: kafka_key
  - src: kafka_offset
    dst: kafka_offset
  - src: kafka_partition
    dst: kafka_partition
  - src: kafka_timestamp
    dst: kafka_timestamp
    # Eksempel på konverteringsfunksjon
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