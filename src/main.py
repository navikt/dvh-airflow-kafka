import logging
import traceback
import os
from dotenv import load_dotenv
from argparse import ArgumentParser
from typing import Text
import yaml
from .mapping import Mapping
from .transform import Transform
from .kafka_source import KafkaSource
from .oracle_target import OracleTarget
import environment
from .console_target import console_target
from .config import set_secrets_as_envs, SecretConfig

parser = ArgumentParser()
parser.add_argument("-l", "--local", action="store_true")
parser.add_argument("-c", "--console", action="store_true")

args = parser.parse_args()
if args.local:
    load_dotenv("local.env")
    environment.isNotLocal = False
else:
    project_secret_path = os.environ.get("PROJECT_SECRET_PATH", None)
    if project_secret_path:
        set_secrets_as_envs()
    else:
        config = SecretConfig(
            source_secret_path=os.environ["SOURCE_SECRET_PATH"],
            target_secret_path=os.environ["TARGET_SECRET_PATH"],
        )
        config.load_secrets_to_env()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    format='{"msg":"%(message)s", "time":"%(asctime)s", "level":"%(levelname)s"}',
    force=True,
    level=logging.getLevelName(LOG_LEVEL),
)


def kafka_to_oracle_etl_mapping(config: Text):
    config = yaml.safe_load(stream=config)
    source = KafkaSource(config["source"])
    if args.console:
        target = console_target(config["target"])
    else:
        target = OracleTarget(config["target"])
    transform = Transform(config["transform"])
    return Mapping(source, target, transform)


def main() -> None:
    """Main consumer thread"""
    try:
        # run_arguments()
        kafka_to_oracle_etl_mapping(os.environ["CONSUMER_CONFIG"]).run()
    except Exception as ex:
        error_text = ""
        if os.getenv("CONSUMER_LOG_LEVEL") == "debug":
            error_text = traceback.format_exc()
        else:
            trace = []
            tb = ex.__traceback__
            while tb is not None:
                trace.append(
                    str(tb.tb_frame.f_code.co_filename)
                    + " "
                    + str(tb.tb_frame.f_code.co_name)
                    + " "
                    + str(tb.tb_lineno)
                )
                tb = tb.tb_next
            error_text = str(type(ex).__name__) + "\n" + "\n".join(trace)
        logging.error(error_text.replace("\n", " "))
        raise Exception("something went wrong")


if __name__ == "__main__":
    main()
