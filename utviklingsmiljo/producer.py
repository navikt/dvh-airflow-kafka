import json
import glob
import uuid
import random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

meldinger = glob.glob("./kafka-meldinger/melding-*.json")

for melding in meldinger:
    with open(melding, "r", encoding="utf-8") as jsonfile:
        data = None
        file = jsonfile.read()
        if file:
            tmp_dict = json.loads(file)
            data = json.dumps(tmp_dict).encode("utf-8")
        producer.send(topic="test", key="blabla".encode("utf-8"), value=data)
    jsonfile.close()
producer.flush()
