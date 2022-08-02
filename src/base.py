from typing import Generator, Dict, Text, Any, List


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

    def write_batch(self, *args, **kwargs) -> None:
        raise NotImplementedError

    def get_kv_from_config_by_method(self, method):
        if self.config.get("custom-config"):
            return {
                v["name"]: eval(v["value"])
                for v in self.config.get("custom-config")
                if v["method"] == method
            }
        return {}
