#! /bin/bash

set -euxo pipefail

RELEASE_TAG="0.2"

docker build -t ilyail3/lt_kafka:latest .
docker tag ilyail3/lt_kafka:latest ilyail3/lt_kafka:${RELEASE_TAG}
docker push ilyail3/lt_kafka:latest
docker push ilyail3/lt_kafka:${RELEASE_TAG}