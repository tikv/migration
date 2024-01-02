#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
SINK_TYPE=$1
CDC_BINARY=tikv-cdc.test
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	kafka) SINK_URI=$(get_kafka_sink_uri "$TEST_NAME") ;;
	*) SINK_URI="" ;;
	esac

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8600" --pd $UP_PD
	start_ts=$(get_start_ts $UP_PD)
	changefeed_id=$(tikv-cdc cli changefeed create --pd=$UP_PD --start-ts=$start_ts --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "$SINK_URI"
	fi

	for i in $(seq 1 10); do
		tikv-cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$UP_PD
		rawkv_op $UP_PD put 5000
		tikv-cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$UP_PD
		check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

		tikv-cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$UP_PD
		rawkv_op $UP_PD delete 5000
		tikv-cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$UP_PD
		check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	done

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
