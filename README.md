# DVH Airflow Kafka
dvh-airflow-kafka kan benyttes for å konsumere data fra kafka-topics i Aiven og skrive data til tabeller i Oracle on-prem database (DWH). Dette er typisk aktuelt for "dvh"-team som bygger dataprodukter basert på data i "datavarehuset".


Følg denne [guiden](kafka-topic.md) for å opprette en NAIS applikasjon som har tilgang til ønsket topic.

# Lokal utvikling
Opprett utviklingsmiljøet:

```bash
git clone ...
cd dvh_ariflow_kafka
uv sync
```

## Kjøre tester

Krever at [Docker desktop](https://www.docker.com/products/docker-desktop/) (Krever lisen) og [uv](https://docs.astral.sh/uv/getting-started/installation/) er installert. \
[Podman](https://podman-desktop.io/tutorial/testcontainers-with-podman) er et godt alternativ til Docker Desktop.
- [Podman desktop](https://podman-desktop.io/docs/migrating-from-docker/managing-docker-compatibility)
- [env-vars-fix](https://stackoverflow.com/questions/67642620/docker-credential-desktop-not-installed-or-not-available-in-path)


```bash
uv run pytest src
uv run pytest src/development/test_integration.py
uv run pytest src/development/test_integration.py::test_test_run_subscribe
```

Sett miljøvariablen `KEEP_CONTAINERS` til `true` for å beholde containerne etter kjøring av tester. Da kjører testene mye raskere, men man må selv rydde opp i containerne etterpå. Når man omstarter docker/podman, må man selv starte containerne på nytt. Navn på containere er `testcontainer-dvh-airflow-kafka-broker` og `testcontainer-dvh-airflow-kafka-oracle-db`.

## Kjøre i KNAST

I KNAST konfigurasjon, åpne opp brannmuren for databasen og aiven/kafka. Din bruker trenger tilgang til kafka hemmeligheten i Google Secret Manager og proxytilgang til databaseskjemaet.

Skriv konsumentkonfigurasjonen til en sti på disken, ref. dokumentasjon om kjøring i airflow.

```bash
# Konsumentkonfigurasjon
export CONSUMER_CONFIG_PATH="/path/to/consumer_config.yml"
# GSM hemmelighet med kafka konfigurasjon
export SOURCE_SECRET_PATH="projects/{PROJECT_ID}/secrets/{KAFKA_SECRET_NAME}/versions/latest"
# GSM hemmelighet med db konfigurasjon
export TARGET_SECRET_PATH="projects/{PROJECT_ID}/secrets/{DB_SECRET_NAME}/versions/latest"
# Vi kjører ikke i airflow
export ENVIRONMENT="LOCAL"
# Overskriv DB_USER from DB_SECRET_NAME - da hentes passordet fra en terminal prompt
export DB_USER="{IDENT}[{SCHEMA}]"
# Skriv XCOM til denne filen, sett den til en tom streng dersom den ikke skal skrives
export XCOM_TARGET_PATH="xcom.json"
```

For å forenkle utviklingen, skriv alle export uttrykk til `env.sh` og kjør `source env.sh` for å sette de.

Kjør skriptet med:
```bash
uv run python -m src.main
```

## Bruke kafka-konsument i Airflow
DVH-AIRFLOW-KAFKA bruker `google secret manager` for å laste inn hemligheter for å koble til et kafka topic og oracle. Du angir navnet på hemlighetene slik at konsumenten kan laste inn hemlighetene som miljøvariabler. Som bruker har du disse mulighetene:
1. `SOURCE_SECRET_PATH` & `TARGET_SECRET_PATH`
2. `PROJECT_SECRET_PATH`

Vi anbefaler å legge inn kafka hemligheten i `SOURCE_SECRET_PATH` og oracle hemlighetene i `TARGET_SECRET_PATH`. Alternativt kan de kombineres i en hemliget som angis av `PROJECT_SECRET_PATH`

Airflow-operatorer [dv-a-team-dags/operators/kafka_operator.py](https://github.com/navikt/dv-a-team-dags/blob/main/operators/kafka_operator.py)

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

DVH-AIRFLOW-KAFKA forventer en miljøvariabler `CONSUMER_CONFIG` der verdien er en streng på `yaml` format. Det er denne som bestemmer hvor dataen hentes fra, hvordan den transformeres, og hvor den lagres. Alternativ kan den spesifiseres som fil med stien in `CONSUMER_CONFIG_PATH`.

Konsumenten prosesserer alle meldinger i topicet. Dersom en melding feiler under prosessering, vurderes om feilen er kritisk eller ikke kan prøves på nytt. I såfall avsluttes kjøringen. Dersom feilen ikke er kritisk og den kan prøves på nytt, så registreres feilen og prosesseringen fortsetter. Etter endt prosessering skriver konsumenten slike feilmeldinger til loggen og avslutter med suksess (exit kode 0).

Informasjon om kjøringen logges til xcom (`/airflow/xcom/return.json`, or `XCOM_TARGET_PATH`):
```json
{
    "event_count": ...,
    "data_count": ...,
    "written_to_db_count": ...,
    "committed_to_producer_count": ...,
    "error_count": ...,
    "empty_count": ...,
    "non_empty_count": ...
}
```


For å avslutte prosessen med feil (exit kode 1) når det er flere enn én ikke-kritisk feil, sett miljøvariablen `FAIL_ON_NON_CRITICAL_ERROR` til `true`.

Kodeeksempler [dv-a-team-dags/consumer_configs](https://github.com/navikt/dv-a-team-dags/tree/main/consumer_configs)

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
  - src: $$BATCH_TIME # use $$$BATCH_TIME in airflow
    dst: lastet_tid
```

# Database schema
Måltabellen må ha følgende kolonner:
```json
create table raa_my_topic_strom (
    kafka_key varchar2(200),
    kafka_offset number(38),
    kafka_partition number(38),
    kafka_timestamp timestamp(6),
    kafka_topic varchar2(200),
    kafka_hash varchar2(200),
    kafka_message blob,
    kildesystem varchar2(200),
    lastet_tid timestamp,
)
```


# Kafka-utviklings-CLI

CLI-et kan brukes til å hente meldinger, inspisere consumer group-offsets og finne eller endre offsets uten å kjøre
ETL-jobben. Det henter Kafka-legitimasjonen fra Google Secret Manager (GSM), og kildekonfigurasjonen fra en lokal
JSON- eller YAML-fil.

Lokal kjøring krever tilgang til GSM via Application Default Credentials og tillatelse til å lese den aktuelle
hemmeligheten. Secret resource name må inkludere versjon, for eksempel
`projects/my-project/secrets/kafka-credentials/versions/latest`. Hemmelighetens verdi skal være et JSON-objekt med
Kafka-miljøvariabler som `KAFKA_BROKERS`, `KAFKA_CERTIFICATE`, `KAFKA_PRIVATE_KEY` og `KAFKA_CA`. For Avro kreves
også `KAFKA_SCHEMA_REGISTRY`, `KAFKA_SCHEMA_REGISTRY_USER` og `KAFKA_SCHEMA_REGISTRY_PASSWORD`.

Angi secret resource name med `--project-secret-path` eller miljøvariabelen `PROJECT_SECRET_PATH`.
Konfigurasjonsfilen angis med `--config` eller `CONSUMER_CONFIG_PATH`. Filen kan være den vanlige YAML-konfigurasjonen
med en `source:`-seksjon, eller bare kildekonfigurasjonen. Den må inneholde blant annet `type`, `batch-size`,
`topic`, `group-id` og `schema`.

Eksempel på en kildekonfigurasjon yaml fil:

```yaml
type: kafka
batch-size: 100
topic: topic-navn
group-id: min-consumer-group
schema: json
```

Sett variablene én gang, og kjør deretter kommandoene fra repository-roten.

Bruk `jq` for å formatere JSON-linjer.

```bash
export PROJECT_SECRET_PATH="projects/my-project/secrets/kafka-credentials/versions/latest"
export CONSUMER_CONFIG_PATH="/home/user/my_project/source.yaml"
export PYTHON_PATH=$(pwd):$PYTHON_PATH

# Hent én melding. Offset må kombineres med én partisjon.
uv run python -m src.kafka_cli fetch --partition 0 --offset 123 | jq

# Hent offset-intervallet [123, 150): slutt-offseten er eksklusiv.
uv run python -m src.kafka_cli fetch --partition 0 --start-offset 123 --end-offset 150 | jq

# Hent meldinger i et tidsintervall med tidssone fra alle partisjoner.
uv run python -m src.kafka_cli fetch --start-time "2026-10-07T08:00:00+02:00" --end-time "2026-10-07T09:00:00+02:00" --limit 1000 | jq

# Vis lagrede offsets, vannmerker og estimert etterslep for consumer group.
uv run python -m src.kafka_cli offsets | jq

# Finn offset-grenser som inneholder tidsintervallet.
uv run python -m src.kafka_cli offsets-for-times --start-time "2026-10-07T08:00:00+02:00" --end-time "2026-10-07T09:00:00+02:00" | jq

# Forhåndsvis ny offset (gjør ingen endringer uten --execute).
uv run python -m src.kafka_cli set-offset --partition 0 --offset 123 | jq

# Endre offset etter at gruppens konsumenter er stoppet.
uv run python -m src.kafka_cli set-offset --partition 0 --offset 123 --execute | jq
```

Alternativt kan ressursnavnet sendes direkte i CLI-et:

```bash
uv run python -m src.kafka_cli \
    --project-secret-path "projects/my-project/secrets/kafka-credentials/versions/latest" \
    --config "/home/user/my_project/source.yaml" \
    offsets | jq
```

Tidsflaggene godtar Unix-tid i millisekunder eller ISO-8601 med tidssone, for eksempel `2026-10-07T08:00:00Z` for UTC eller `2026-10-07T08:00:00+02:00` for en tidssone med forskyvning på +02:00. ISO-8601 uten tidssone godtas ikke. Tidsintervaller er halvåpne: starttidspunktet er inkludert og sluttidspunktet er
ekskludert. `fetch` leser direkte fra partisjonene, bruker ikke consumer groupens lagrede posisjon og committer ikke
offsets. Resultatene skrives som JSON-linjer til stdout; logger skrives til stderr. Meldingsverdiene dekodes etter
`schema`-innstillingen, men det anvendes ingen filtrering hvis det er konfigurert til produksjonsbruk.

`set-offset` endrer denne lagrede posisjonen, ikke posisjonen til en allerede kjørende konsument. `--execute` krever at gruppen er
inaktiv; hold den stoppet til den tiltenkte konsumenten startes igjen. `--partition` kan gjentas for å velge flere
partisjoner og gjelder alle kommandoene; uten flagget velges alle partisjoner.

`fetch` har standardgrense på 100 meldinger og logger en advarsel dersom grensen nås.

`offsets` viser committed offset, som er posisjonen neste konsument i gruppen vil lese fra.

Eksempel for `offsets` kommandoen:
```json
{
  "topic": "paw.arbeidssokerperioder-v1",
  "partition": 0,
  "group_id": "dvh_arb_reg_periode_konsument",
  "committed_offset": 8161,
  "earliest_offset": 0,
  "end_offset": 8169,
  "lag": 8,
  "timestamp_earliest_offset": "2024-03-21 09:49:47.955000+00:00",
  "last_committed_message": {
    "offset": 8160,
    "timestamp": "2026-10-16T03:21:48.123500+00:00"
  },
  "last_message": {
    "offset": 8167,
    "timestamp": "2026-10-16T07:23:27.751000+00:00"
  }
}
```