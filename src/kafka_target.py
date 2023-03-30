import os
from typing import Dict, Text, Any, List
from base import Target
from confluent_kafka import Producer
import environment
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


class KafkaTarget(Target):
    def __init__(self, config: Dict[Text, Any]) -> None:
        super().__init__(config)
        self.connection = self.create_connection()


    def create_connection(self) -> Producer:
        pass


    def write_batch(self, batch: List[Dict[Text, Any]]) -> None:
        pass


class AvroKafkaTarget(KafkaTarget):

    def __init__(self, config: Dict[Text, Any]) -> None:
        super().__init__(config)

    def create_connection(self) -> Producer:
        return AvroProducer(self._kafka_config())

    def _kafka_config(self):
        config = {
            "bootstrap.servers": os.environ["KAFKA_BROKERS"],
            "schema.registry.url": os.environ["KAFKA_SCHEMA_REGISTRY"],
            #"key.schema": avro.loads(self.config['AVRO_KEY_SCHEMA']),
            "value.schema": avro.loads(os.environ['AVRO_MESSAGE_SCHEMA'])
        }

        if environment.isNotLocal:
            config.update({
                "ssl.certificate.location": os.environ["KAFKA_CERTIFICATE_PATH"],
                "ssl.key.location": os.environ["KAFKA_PRIVATE_KEY_PATH"],
                "ssl.ca.location": os.environ["KAFKA_CA_PATH"],
                "security.protocol": "SSL",
                "basic.auth.credentials.source": "USER_INFO",
                "basic.auth.user.info": "{}:{}".format(os.environ["KAFKA_SCHEMA_REGISTRY_USER"], os.environ["KAFKA_SCHEMA_REGISTRY_PASSWORD"])
            })
        return config



    def write_batch(self, batch: List[Dict[Text, Any]]) -> None:
        for message in batch:
            self.connection.produce(self.config['topic'], value=message)