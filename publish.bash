#! /bin/bash

set -euxo pipefail

docker build -t ilyail3/lt_kafka:latest .
docker tag ilyail3/lt_kafka:latest ilyail3/lt_kafka:0.1
docker push ilyail3/lt_kafka:latest
docker push ilyail3/lt_kafka:0.1