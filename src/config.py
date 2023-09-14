import os
from typing import Optional
from google.cloud import secretmanager
import json


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

    def _set_secret_as_env(secret_name: str) -> None:
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_version = secret_client.access_secret_version(name=secret_name)
        secret_payload = secret_version.payload.data.decode("UTF-8")
        secret_dict = json.loads(secret_payload)
        os.environ.update(secret_dict)

    def load_secrets_to_env(self):
        self._set_secret_as_env(secret_name=self.source_secret_path)
        self._set_secret_as_env(secret_name=self.target_secret_path)
