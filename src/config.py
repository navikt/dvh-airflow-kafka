import os
import json
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict
from google.cloud import secretmanager


def set_secrets_as_envs():
    # Set the default resource name if PROJECT_SECRET_PATH is not provided
    default_resource_name = f"{os.environ['KNADA_TEAM_SECRET']}/versions/latest"
    resource_name = os.environ.get("PROJECT_SECRET_PATH", default_resource_name)

    secret_client = secretmanager.SecretManagerServiceClient()
    secret_version = secret_client.access_secret_version(name=resource_name)
    secret_payload = secret_version.payload.data.decode("UTF-8")
    secret_dict = json.loads(secret_payload)
    os.environ.update(secret_dict)


class SecretConfig:
    def __init__(
        self,
        source_secret_path: str,
        target_secret_path: str,
    ):
        self.source_secret_path = source_secret_path
        self.target_secret_path = target_secret_path

    @staticmethod
    def _set_secret_as_env(secret_name: str) -> None:
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_version = secret_client.access_secret_version(name=secret_name)
        secret_payload = secret_version.payload.data.decode("UTF-8")
        secret_dict = json.loads(secret_payload)
        os.environ.update(secret_dict)

    def load_secrets_to_env(self):
        self._set_secret_as_env(secret_name=self.source_secret_path)
        self._set_secret_as_env(secret_name=self.target_secret_path)


class SchemaType(StrEnum):
    AVRO = "avro"
    JSON = "json"
    STRING = "string"


class SourceType(StrEnum):
    KAFKA = "kafka"


class KeyDecoder(StrEnum):
    INT_64 = "int-64"
    UTF_8 = "utf-8"


class TargetType(StrEnum):
    ORACLE = "oracle"


class SourceConfig(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    type: SourceType
    batch_size: int = Field(alias="batch-size")
    batch_interval: int = Field(alias="batch-interval")
    topic: str
    group_id: str = Field(alias="group-id")
    schema_type: SchemaType = Field(alias="schema")
    key_decoder: KeyDecoder = Field("utf-8", alias="key-decoder")
    keypath_separator: Optional[str] = Field(None, alias="keypath-seperator")
    message_fields_filter: Optional[list] = Field([], alias="message-fields-filter")


class TargetConfig(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    type: TargetType
    custom_config: list = Field(alias="custom-config")
    delta: dict
    table: str
    skip_duplicates_with: Optional[list] = Field([], alias="skip-duplicates-with")
