#!/bin/bash

set -eu

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

	export GO_FAILPOINTS='github.com/tikv/migration/cdc/cdc/sink/SinkFlushEventPanic=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8600" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	run_cdc_cli changefeed create --sink-uri="$SINK_URI"
	sleep 10
	ensure 10 "tikv-cdc cli processor list|jq '.|length'|grep -E '^1$'"

	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2" --addr "127.0.0.1:8601" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
	ensure 10 "tikv-cdc cli processor list|jq '.|length'|grep -E '^1$'"
	ensure 10 "tikv-cdc cli capture list|jq '.|length'|grep -E '^2$'"

	rawkv_op $UP_PD put 10000
	# wait cdc server 1 is panic
	ensure 10 "tikv-cdc cli capture list|jq '.|length'|grep -E '^1$'"

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs_contains $WORK_DIR "tikv sink injected error" 1
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
