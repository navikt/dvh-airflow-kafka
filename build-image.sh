#!/bin/sh
set -e

echo "Specify a new version number (example. 1.2.1) for the image. \
See https://semver.org for more info and \
https://github.com/navikt/statistikkforsykmelder/packages/728683 for the latest \
version number:"
read version

echo "Authenticate to GitHub container registry (ghcr.io)"
docker login ghcr.io

docker build . -t ghcr.io/navikt/dvh-kafka-airflow-consumer:$version
docker push ghcr.io/navikt/dvh-kafka-airflow-consumer:$version
