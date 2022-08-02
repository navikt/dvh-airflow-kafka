from base import Source, Target
from transform import Transform


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

    def run_mapping(self, once: bool = False) -> None:
        for batch in self.source.read_batches():
            self.target.write_batch(list(map(self.transform, batch)))
