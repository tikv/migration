#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1,http://$UP_PD_HOST_2:$UP_PD_PORT_2,http://$UP_PD_HOST_3:$UP_PD_PORT_3
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT
RETRY_TIME=10
function restart_cdc() {
	id=$1
	count=$(ps -aux | grep "tikv-cdc.test" | grep "cdc$id.log" | wc | awk '{print $1}')
	if [ "$count" -eq 0 ]; then
		echo "restart cdc$id"
		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "$id" --addr "127.0.0.1:860$id" --pd "$UP_PD"
	fi
}

function check_capture_count() {
	expected=$1

	for ((i = 0; i <= 10; i++)); do
		count=$(tikv-cdc cli capture list --pd=$UP_PD | jq '.|length')
		if [[ "$count" == "$expected" ]]; then
			echo "check capture count successfully"
			break
		fi
		if [ "$i" -eq 10 ]; then
			echo "failed to check capture count, expected: $expected, got: $count"
			exit 1
		fi
		echo "failed to check capture count, expected: $expected, got: $count, retry: $i"
		# when sent SIGSTOP to pd leader, cdc maybe exit that is expect, and we
		# shoule restart it
		restart_cdc 1
		restart_cdc 2
		sleep 10
	done
}

export -f check_capture_count

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR --multiple-upstream-pd "true"
	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8600" --pd "$UP_PD"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2" --addr "127.0.0.1:8601" --pd "$UP_PD"

	for i in {1..10}; do
		echo "cdc_hang_on test $i"
		name=$(pd-ctl member --pd=$UP_PD | jq ."leader" | jq ."name" | tr -d '"')
		pid=$(ps -aux | grep "name=$name" | awk '{print $2}' | head -n1)
		kill -19 $pid
		sleep 10
		check_capture_count 2
		kill -18 $pid
		sleep 10
	done

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
