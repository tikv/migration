#!/usr/bin/env bash
# fork from https://github.com/pingcap/tidb/blob/master/br/tests/up.sh
#
# Use for creating a container to run integrated tests in a isolated environment.
#
# Usage:
# 1. Run up.sh from cdc directory: tests/up.sh
# 2. Run: make integration_test
#

set -eo pipefail

IMAGE_TAG="nightly"
while [[ $# -gt 0 ]]; do
	key="$1"

	case $key in
	--tag)
		IMAGE_TAG=$2
		shift
		shift
		;;
	--cleanup-docker)
		CLEANUP_DOCKER=1
		shift
		;;
	--cleanup-data)
		CLEANUP_DATA=1
		shift
		;;
	--cleanup-all)
		CLEANUP_ALL=1
		shift
		;;
	--help)
		HELP=1
		shift
		;;
	*)
		HELP=1
		break
		;;
	esac
done

if [ "$HELP" ]; then
	echo "Usage: $0 [OPTIONS]"
	echo "OPTIONS:"
	echo "  --help                 Display this message"
	echo "  --tag (TAG)            Specify images tag used in tests"
	echo "  --cleanup-docker       Clean up tests Docker containers"
	echo "  --cleanup-data         Clean up persistent data"
	echo "  --cleanup-all          Clean up all data inlcuding Docker images, containers and persistent data"
	exit 0
fi

host_tmp=/tmp/tikv_cdc_test_$USER
host_bash_history=$host_tmp/bash_history

# Persist tests data and bash history
mkdir -p "$host_tmp"
touch "$host_bash_history" || true
function cleanup_data() {
	rm -rf "$host_tmp" || {
		echo try "sudo rm -rf $host_tmp"?
		exit 1
	}
}
if [ "$CLEANUP_DATA" ]; then
	cleanup_data
	exit 0
fi

# Clean up docker images and containers.
docker_repo=tikv_cdc_tests
function cleanup_docker_containers() {
	containers=$(docker container ps --all --filter="ancestor=$docker_repo:$IMAGE_TAG" -q)
	if [ "$containers" ]; then
		docker stop "$containers"
		docker rm "$containers"
	fi
}
function cleanup_docker_images() {
	images=$(docker images --filter="reference=$docker_repo:$IMAGE_TAG" -q)
	if [ "$images" ]; then
		docker rmi "$images"
	fi
}
if [ "$CLEANUP_DOCKER" ]; then
	cleanup_docker_containers
	exit 0
fi

if [ "$CLEANUP_ALL" ]; then
	cleanup_data
	cleanup_docker_containers
	cleanup_docker_images
	exit 0
fi

docker build -t "$docker_repo":"$IMAGE_TAG" -f tests/tests.Dockerfile .

# Start an existing container or create and run a new container.
exist_container=$(docker container ps --all -q --filter="ancestor=$docker_repo:$IMAGE_TAG" --filter="status=exited" | head -n 1)
if [ "$exist_container" ]; then
	docker start "$exist_container"
	echo "Attach exsiting container: $exist_container"
	exec docker attach "$exist_container"
else
	volume_args=()
	for f in * .[^.]*; do
		volume_args=("${volume_args[@]} -v $(pwd)/$f:/cdc/$f")
	done
	echo "Run a new container"
	echo "Run \"make integration_test\" to start integrated tests"
	# shellcheck disable=SC2068
	exec docker run -it \
		-v "$host_tmp":/tmp/tikv_cdc_test \
		-v "$host_bash_history":/root/.bash_history \
		${volume_args[@]} \
		--cpus=8 --memory=16g \
		"$docker_repo":"$IMAGE_TAG"
fi
