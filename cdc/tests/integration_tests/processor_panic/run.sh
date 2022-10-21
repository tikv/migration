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

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix 1 --addr 127.0.0.1:8600 --restart true \
		--failpoint 'github.com/tikv/migration/cdc/cdc/processor/pipeline/ProcessorSyncResolvedPreEmit=return(true)'

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac
    # Make sure the task is assigned to the first cdc
	run_cdc_cli changefeed create --sink-uri="$SINK_URI"
    sleep 10

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix 2 --addr 127.0.0.1:8601

	rawkv_op $UP_PD put 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*

check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
