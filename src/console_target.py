from typing import Dict, Text, Any, List, Tuple
from .base import Target


class console_target(Target):

    def create_connection(self) -> object:
        pass

    def write_batch(self, batch: List[Dict[Text, Any]]) -> None:
        print(batch)

    def get_kode67(self, batch: List[Dict[Text, Any]]) -> List[Tuple]:
        k67_list = [(1,), (2,)]
        return k67_list
