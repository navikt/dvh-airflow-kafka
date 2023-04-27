#Liste topics
#docker exec -it $(docker ps -aqf "name=zookeeper") /bin/zookeeper-shell localhost:2181 ls /brokers/topics
#Slette test topic
docker exec -it $(docker ps -aqf "name=zookeeper") /bin/zookeeper-shell localhost:2181 deleteall /brokers/topics/test

## Produsere meldinger til test topic
kafkacat -b localhost:9092 -t test -T -P ./kafka-meldinger/melding-hello.json

kafkacat -b localhost:9092 -t test -T -P ./kafka-meldinger/melding-k6.json

for i in {1..10}
do
    kafkacat -b localhost:9092 -t test -T -P ./kafka-meldinger/cv.json
done
