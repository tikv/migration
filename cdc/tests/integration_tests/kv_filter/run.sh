#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	start_ts=$(get_start_ts $UP_PD)
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	uuid="custom-changefeed-name"
	tikv-cdc cli changefeed create \
		--start-ts=$start_ts \
		--sink-uri="$SINK_URI" \
		--changefeed-id="$uuid" \
		--config $CUR/conf/changefeed.toml

	rawkv_op $UP_PD put 5000

	# Filter configured in $CUR/conf/changefeed.toml will match events with key >= 3000
	# So wait for sync finished, pause changefeed, delete keys < 3000 for upstream, then check_sync_diff
	sleep 1 && check_total_kvs $WORK_DIR $DOWN_PD 2000
	run_cdc_cli changefeed --changefeed-id $uuid pause
	rawkv_op $UP_PD delete 3000

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
