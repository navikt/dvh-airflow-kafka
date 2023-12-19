from typing import Generator, Dict, Text, Any, List, Tuple
from config import SourceConfig, TargetConfig

import oracledb


class Source:
    connection_class: Any = NotImplemented

    def __init__(self, config: Dict[Text, Any]) -> None:
        self.config = SourceConfig(**config).model_dump(by_alias=True)

    def read_batches(
        self, *args, **kwargs
    ) -> Generator[List[Dict[Text, Any]], None, None]:
        raise NotImplementedError

    def read_once(self, *args, **kwargs) -> Dict[Text, Any]:
        raise NotImplementedError


class Target:
    connection_class: Any = NotImplemented

    def __init__(self, config: Dict[Text, Any]) -> None:
        self.config = TargetConfig(**config).model_dump(by_alias=True)
        # self.connection = self.create_connection()

    def create_connection(self) -> object:
        raise NotImplementedError

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

    def get_latest_timestamp_for_delta(self) -> Text:
        raise NotImplementedError

    def metadata(batch):
        """
        Takes a batch of kafka source data and logs the metadata. That is all the
        columns with names starting with 'kafka_'. The other colmns are ignored.
        Assumes that no columns starting with 'kafka_' contains any personal information.

        Parameters:
            batch (List[Dict[Text, Any]]): A batch of data
        """
        header = [header for header in batch[0].keys() if "message" not in header]
        header_str = "".join([f"{key:>30}" for key in header])
        log_list = [header_str]
        for row in batch:
            row_str = "".join([f"{row[key]:>30}" for key in header])
            log_list.append(row_str)
        log_str = "\n".join(log_list)
        return log_str
