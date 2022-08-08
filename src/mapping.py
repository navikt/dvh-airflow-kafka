from base import Source, Target
from transform import Transform
import json


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

    def run_mapping(self, once: bool = False) -> None:
        total_messages = 0
        for batch in self.source.read_batches():
            total_messages += len(batch)
            kode67 = self.target.get_kode67(batch)
            kode67_personer = set()
            for personer in kode67:
                for person in personer:
                    kode67_personer.add(person)
            for msg in batch:
                kafka_message = json.loads(msg["kafka_message"])
                if (
                    kafka_message[self.target.config["k6-filter"]["col"]]
                    in kode67_personer
                ):
                    msg["kafka_message"] = None
            self.target.write_batch(list(map(self.transform, batch)))
        with open("/airflow/xcom/return.json", "w") as xcom:
            xcom.write(str(total_messages))
