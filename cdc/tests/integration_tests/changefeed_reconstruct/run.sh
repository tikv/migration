#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT
SINK_TYPE=$1
MAX_RETRIES=10

function check_no_capture() {
	pd=$1
	count=$(cdc cli capture list --pd=$pd 2>&1 | jq '.|length')
	if [[ ! "$count" -eq "0" ]]; then
		exit 1
	fi
}

export -f check_no_capture

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8600" --logsuffix server1 --pd $UP_PD
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	start_ts=$(get_start_ts $UP_PD)
	changefeed_id=$(tikv-cdc cli changefeed create --pd=$UP_PD --start-ts=$start_ts --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	rawkv_op $UP_PD put 5000

	# kill capture
	kill $owner_pid
	ensure $MAX_RETRIES check_no_capture $UP_PD

	# run another cdc server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8601" --logsuffix server2
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --pd=$UP_PD 2>&1 | grep id"
	capture_id=$($CDC_BINARY cli --pd=$UP_PD capture list 2>&1 | awk -F '"' '/id/{print $4}')
	echo "capture_id:" $capture_id

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
