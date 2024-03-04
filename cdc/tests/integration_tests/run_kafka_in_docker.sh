#!/bin/bash
# Usage:
#   ./tests/integration_tests/run_kafka_in_docker.sh --case [test_names]

set -euo pipefail

CASE="*"

while [[ $# -gt 0 ]]; do
	key="$1"

	case $key in
	--case)
		CASE=$2
		shift
		shift
		;;
	esac
done

COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 \
	CASE="$CASE" \
	docker-compose --compatibility -f ./deployments/tikv-cdc/docker-compose/docker-compose-kafka-integration.yml up --build
