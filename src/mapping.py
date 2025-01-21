import os
import logging
from datetime import datetime, timezone

from confluent_kafka import KafkaError, TopicPartition

from .kafka_source import KafkaSource
from .oracle_target import OracleTarget
from .transform import Transform
from .config import KafkaConsumerStrategy


class Mapping:
    """ETL Mapping Class"""

    def __init__(
        self,
        source: KafkaSource,
        target: OracleTarget,
        transform: Transform,
    ) -> None:
        """
        A container class for a Source-, and Target system
        with an associated data Transform, i.e.,
        a model class of the classic ETL Mapping.

        Arguments:
            source: instance of Source system implementing read_batches()
            target: instance of Target system implementing write_batch()
            transform: instance of callable Transform
        """
        self.source = source
        self.target = target
        self.transform = transform
        self.total_messages = 0
        self.start_timestamp = int(datetime.now(timezone.utc).timestamp())

    def run_assign(self) -> None:
        total_messages = 0
        for batch in self.source.read_polled_batches():
            total_messages += len(batch)
            self.target.write_batch(list(map(self.transform, batch)))
        if os.environ["ENVIRONMENT"] != "LOCAL":
            with open("/airflow/xcom/return.json", "w") as xcom:
                xcom.write(str(total_messages))

    def run_subscribe(self) -> None:
        total_messages = 0
        consumer = self.source.get_consumer()
        consumer.subscribe([self.source.config.topic])
        batch = []
        while True:

            m = consumer.poll(
                timeout=self.source.config.poll_timeout,
            )

            if m == None:  # No messages
                logging.info("End of kafka log. Exiting")
                break

            err: KafkaError | None = m.error()
            if err:
                logging.warning(f"Message returned error {err}")
            else:  # Handle proper message
                batch.append(self.source.collect_message(msg=m))

            if len(batch) == self.source.config.batch_size:
                self.target.write_batch(list(map(self.transform, batch)))  # Write batch to Oracle
                logging.info("Committing offset after batch insert")
                consumer.commit()
                total_messages += len(batch)
                batch = []

        self.target.write_batch(list(map(self.transform, batch)))  # Write batch to Oracle
        total_messages += len(batch)
        logging.info("Committing offset after last batch insert")
        logging.info(f"{total_messages} messages consumed")
        consumer.commit()
        consumer.close()

    def run(self) -> None:
        if self.source.config.strategy == KafkaConsumerStrategy.ASSIGN:
            return self.run_assign()
        if self.source.config.strategy == KafkaConsumerStrategy.SUBSCRIBE:
            return self.run_subscribe()
