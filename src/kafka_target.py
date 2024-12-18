import os
from typing import Dict, Text, Any, List
from .base import Target
from confluent_kafka import Producer

from uuid import uuid4
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


def chk_dict(dikt, ctx) -> Dict[Text, Any]:
    if type(dikt) is not dict:
        raise TypeError("This target only supports dicts!")
    return dikt


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print(
        "User record {} successfully produced to {} [{}] "
        "at offset {}".format(msg.key(), msg.topic(), msg.partition(), msg.offset())
    )


class KafkaTarget(Target):
    def __init__(self, config: Dict[Text, Any]) -> None:
        super().__init__(config)
        self.value_serializer = self._init_value_serializer()
        self.key_serializer = StringSerializer("utf_8")
        self.producer = self.create_connection()

    def _init_value_serializer(self):
        if self.config["schema"] == "avro":
            return self.avro_serializer()
        elif self.config["schema"] == "json":
            raise NotImplementedError
        elif self.config["schema"] == "string":
            raise NotImplementedError
        else:
            raise AssertionError

    def avro_serializer(self):
        schema_client = SchemaRegistryClient({"url": os.environ["KAFKA_SCHEMA_REGISTRY"]})
        schema_str = os.environ["AVRO_MESSAGE_SCHEMA"]
        return AvroSerializer(schema_client, schema_str, chk_dict)

    def create_connection(self) -> Producer:
        return Producer(self._kafka_config())

    def _kafka_config(self):
        config = {
            "bootstrap.servers": os.environ["KAFKA_BROKERS"],
        }
        # print(os.environ["KAFKA_BROKERS"])

        if environment.isNotLocal:
            config.update(
                {
                    "ssl.certificate.location": os.environ["KAFKA_CERTIFICATE_PATH"],
                    "ssl.key.location": os.environ["KAFKA_PRIVATE_KEY_PATH"],
                    "ssl.ca.location": os.environ["KAFKA_CA_PATH"],
                    "security.protocol": "SSL",
                    "basic.auth.credentials.source": "USER_INFO",
                    "basic.auth.user.info": "{}:{}".format(
                        os.environ["KAFKA_SCHEMA_REGISTRY_USER"],
                        os.environ["KAFKA_SCHEMA_REGISTRY_PASSWORD"],
                    ),
                }
            )
        return config

    def write_batch(self, batch: List[Dict[Text, Any]]) -> None:
        self.producer.poll(0.0)
        topic = self.config["topic"]
        ctx = SerializationContext(topic, MessageField.VALUE)
        for message in batch:
            self.producer.produce(
                topic=topic,
                key=self.key_serializer(str(uuid4())),
                value=self.value_serializer(message, ctx),
                on_delivery=delivery_report,
            )
        print(f"\nFlushing records...{self.producer.flush(5)}")
