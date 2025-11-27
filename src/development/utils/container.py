import logging
import os
from contextlib import contextmanager

import docker
from testcontainers.core.container import DockerContainer

logger = logging.getLogger("development.utils.container")


@contextmanager
def get_or_start_container(container_name, test_container: DockerContainer):
    """Start or reuse (if KEEP_CONTAINERS is set) a test container.
    """
    keep_containers = os.getenv('KEEP_CONTAINERS', 'false').lower() == 'true'

    if keep_containers:
        try:
            client = docker.from_env()
            # this will raise an error if the container is not found
            container = client.containers.get(container_name)

            if container.status == 'running':
                logger.info(f"Using running container '{container_name}'.")
            else:
                logger.info(f"Found container '{container_name}' with status '{container.status}', starting it")
                container.start()

            yield

            return

        except docker.errors.NotFound:
            logger.info(f"No existing container with name '{container_name}' found, creating new one")
            pass
        # fall through to start a new container

    # fixed name for reuse and error messages should a container be running from before
    test_container.with_name(container_name)

    # Set the time zone to local, default is UTC which can cause issues when converting to local time
    test_container.with_env("TZ", "Europe/Oslo")
    test_container.with_env("ORA_SDTZ", "Europe/Oslo")

    try:
        logger.info(f"Creating test container '{container_name}'")
        test_container.start()

        yield

    finally:
        if not keep_containers:
            logger.info(f"Stopping and removing test container '{container_name}'")
            test_container.stop()
        else:
            logger.info(f"Test container '{container_name}' is left running. "
                        f"Run `docker container stop {container_name}` to stop it or "
                        f"delete it with `docker container rm -f {container_name}`. "
                        f"Set KEEP_CONTAINERS=false to automatically remove containers after tests.")


def test_oracle_connection(host, port, service, user, password) -> bool:
    """Test the connection to the Oracle database using oracledb."""
    import oracledb
    try:
        # dsn should be a string like "user/password@host:port/service"
        conn = oracledb.connect(host=host, port=port, service_name=service, user=user, password=password)
        with conn.cursor() as cursor:
            cursor.execute("select 1 from dual")
        conn.close()
        return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False