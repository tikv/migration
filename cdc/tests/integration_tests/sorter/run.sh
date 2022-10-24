#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT
# fallback 10s
FALL_BACK=2621440000

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR
	CF_NAME="feed01"

	echo "test unified sorter"
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	start_ts=$(expr $start_ts - $FALL_BACK)
	rawkv_op $UP_PD put 5000

	# Run cdc server with unified sorter.
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac
	run_cdc_cli changefeed create -c $CF_NAME --start-ts=$start_ts --sink-uri="$SINK_URI" --sort-engine="unified"

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
	run_cdc_cli unsafe reset --no-confirm

	echo "test memory sorter"
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	start_ts=$(expr $start_ts - $FALL_BACK)
	rawkv_op $UP_PD put 5000

	# Run cdc server with memory sorter.
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	run_cdc_cli changefeed create -c $CF_NAME --start-ts=$start_ts --sink-uri="$SINK_URI" --sort-engine="memory"

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
