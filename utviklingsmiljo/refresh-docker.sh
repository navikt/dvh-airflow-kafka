#!/bin/bash
#Stoppe og fjerne alle containere
docker ps -aq | xargs docker stop | xargs docker rm

#Starte
docker compose up -d
docker compose up -d db

#Liste topics
#docker exec -it $(docker ps -aqf "name=zookeeper") /bin/zookeeper-shell localhost:2181 ls /brokers/topics
#Slette test topic
docker exec -it $(docker ps -aqf "name=zookeeper") /bin/zookeeper-shell localhost:2181 deleteall /brokers/topics/test