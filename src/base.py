from typing import Generator, Dict, Text, Any, List, Tuple
import oracledb


class Source:

    connection_class: Any = NotImplemented

    def __init__(self, config: Dict[Text, Any]) -> None:
        self.config = config

    def read_batches(
        self, *args, **kwargs
    ) -> Generator[List[Dict[Text, Any]], None, None]:
        raise NotImplementedError


class Target:

    connection_class: Any = NotImplemented

    def __init__(self, config: Dict[Text, Any]) -> None:
        self.config = config

    def write_batch(self, batch: List[Dict[Text, Any]]) -> None:
        raise NotImplementedError

    def get_kode67(self, batch: List[Dict[Text, Any]]) -> List[Tuple]:
        raise NotImplementedError

    def get_kv_from_config_by_method(self, method):
        if self.config.get("custom-config"):
            return {
                v["name"]: eval(v["value"])
                for v in self.config.get("custom-config")
                if v["method"] == method
            }
        return {}
