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

# Espens utviklingsmiljø på WSL2 Ubuntu

## cd til prosjektmappe
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