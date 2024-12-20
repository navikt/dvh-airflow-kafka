import os
import logging

from confluent_kafka import KafkaError

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

    def run_assign(self) -> None:
        total_messages = 0
        for batch in self.source.read_polled_batches():
            total_messages += len(batch)
            k6_conf = self.target.config.k6_filter
            if k6_conf:
                kode67_personer = set(*zip(*self.target.get_kode67(batch)))
                for msg in batch:
                    if msg.get(k6_conf.col) in kode67_personer:
                        msg["kafka_message"] = None
            self.target.write_batch(list(map(self.transform, batch)))
        if os.environ["ENVIRONMENT"] != "LOCAL":
            with open("/airflow/xcom/return.json", "w") as xcom:
                xcom.write(str(total_messages))

    def run_subscribe(self) -> None:
        READ_TO_END = True
        total_messages = 0
        consumer = self.source.get_consumer()
        partitions = {
            key: False
            for key in consumer.list_topics().topics[self.source.config.topic].partitions.keys()
        }
        # Stop når enden er nådd for alle partisjoner
        while not all(partitions.values()):

            messages = consumer.consume(
                num_messages=self.source.config.batch_size,
                timeout=self.source.config.poll_timeout,
            )

            if not messages:  # No messages
                logging.info("End of kafka log. Exiting")
                break
            batch = []
            for m in messages:
                err: KafkaError | None = m.error()
                if err:
                    if err.code() == KafkaError._PARTITION_EOF:
                        logging.info(f"End of log for partition {m.partition()}")
                        partitions[m.partition()] = True
                    else:
                        logging.warning(f"Message returned error {err}")

                else:  # Handle proper message
                    batch.append(self.source.collect_message(msg=m))

            total_messages += len(batch)

            k6_conf = self.target.config.k6_filter
            if k6_conf:
                kode67_personer = set(*zip(*self.target.get_kode67(batch)))
                for msg in batch:
                    if msg.get(k6_conf.col) in kode67_personer:
                        msg["kafka_message"] = None
            self.target.write_batch(list(map(self.transform, batch)))  # Write batch to Oracle
            logging.info("Committing offset after batch insert")
            consumer.commit()

        logging.info("Committing offset after last batch insert")
        logging.info(f"{total_messages} messages consumed")
        consumer.commit()
        consumer.close()

    def run(self) -> None:
        if self.source.config.strategy == KafkaConsumerStrategy.ASSIGN:
            return self.run_assign()
        if self.source.config.strategy == KafkaConsumerStrategy.SUBSCRIBE:
            return self.run_subscribe()
