from base import Source, Target
from transform import Transform
import json
import environment


class Mapping:
    """ETL Mapping Class"""

    def __init__(
        self,
        source: Source,
        target: Target,
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

    def run(self) -> None:
        total_messages = 0
        for batch in self.source.read_batches():
            total_messages += len(batch)
            k6_conf = self.target.config.get("k6-filter")
            if k6_conf:
                kode67_personer = set(*zip(*self.target.get_kode67(batch)))
                for msg in batch:
                    if msg.get(k6_conf["col"]) in kode67_personer:
                        msg["kafka_message"] = None
            self.target.write_batch(list(map(self.transform, batch)))
        if environment.isNotLocal:
            with open("/airflow/xcom/return.json", "w") as xcom:
                xcom.write(str(total_messages))
