import json
import glob
import uuid
import random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

meldinger = glob.glob('./kafka-meldinger/melding-*.json')

for melding in meldinger:
    with open(melding, 'r', encoding='utf-8') as jsonfile:
        tmp_dict = json.load(jsonfile)
        topic = tmp_dict['topic']
        data = json.dumps(tmp_dict)
        producer.send(
            topic,
            value=data.encode('utf-8'),
            key=bytearray(str(random.randint(0,10)).encode('UTF-8'))
        )
    jsonfile.close()
producer.flush()
