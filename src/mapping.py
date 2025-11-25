import logging
from datetime import datetime, timezone

from confluent_kafka import KafkaError, TopicPartition

from .config import KafkaConsumerStrategy
from .kafka_source import KafkaSource, ProcessSummary
from .oracle_target import OracleTarget
from .transform import Transform


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

    def run_assign(self) -> ProcessSummary:
        process_summary = ProcessSummary()
        for batch, process_summary in self.source.read_polled_batches():
            self.target.write_batch(batch, self.transform)

        return process_summary

    def run_subscribe(self) -> ProcessSummary:
        process_summary = ProcessSummary()
        for batch, process_summary in self.source.read_subscribed_batches():
            self.target.write_batch(batch, self.transform)

        return process_summary

    def run(self) -> ProcessSummary:
        if self.source.config.strategy == KafkaConsumerStrategy.ASSIGN:
            process_summary = self.run_assign()
        elif self.source.config.strategy == KafkaConsumerStrategy.SUBSCRIBE:
            process_summary = self.run_subscribe()
        else:
            raise ValueError(
                f"Unsupported Kafka consumer strategy: {self.source.config.strategy}"
            )

        logging.info(f"Process summary: {process_summary}")

        return process_summary