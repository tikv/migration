#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT
MAX_RETRIES=20

function check_changefeed_is_finished() {
	pd=$1
	changefeed=$2
	query=$(tikv-cdc cli changefeed query -c=$changefeed)
	echo "$query"
	state=$(echo "$query" | jq ".state" | tr -d '"')
	if [[ ! "$state" -eq "finished" ]]; then
		echo "state $state is not finished"
		exit 1
	fi

	status_length=$(echo "$query" | sed "/has been deleted/d" | jq '."task-status"|length')
	if [[ ! "$status_length" -eq "0" ]]; then
		echo "unexpected task status length $status_length, should be 0"
		exit 1
	fi
}

export -f check_changefeed_is_finished

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8600" --pd $UP_PD
	now=$(tikv-cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
	# 90s after now
	target_ts=$(($now + 90 * 10 ** 3 * 2 ** 18))
	changefeed_id=$(tikv-cdc cli changefeed create --sink-uri="$SINK_URI" --target-ts=$target_ts 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
    sleep 3

    rawkv_op $UP_PD put 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
    rawkv_op $UP_PD delete 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	sleep 90
	ensure $MAX_RETRIES check_changefeed_is_finished $UP_PD $changefeed_id

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
