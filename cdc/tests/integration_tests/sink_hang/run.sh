#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT

CDC_COUNT=3
MAX_RETRIES=20

function check_changefeed_state() {
	pd_addr=$1
	changefeed_id=$2
	expected=$3
	state=$(tikv-cdc cli --pd=$pd_addr changefeed query -s -c $changefeed_id | jq -r ".state")
	if [[ "$state" != "$expected" ]]; then
		echo "unexpected state $state, expected $expected"
		exit 1
	fi
}

export -f check_changefeed_state

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	kafka) SINK_URI=$(get_kafka_sink_uri "$TEST_NAME") ;;
	*) SINK_URI="" ;;
	esac

	export GO_FAILPOINTS='github.com/tikv/migration/cdc/cdc/sink/SinkExecError=2*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8600" --pd $pd_addr
	start_ts=$(get_start_ts $UP_PD)
	changefeed_id=$(tikv-cdc cli changefeed create --pd=$pd_addr --start-ts=$start_ts --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer --workdir "$WORK_DIR" --upstream-uri "$SINK_URI"
	fi

	rawkv_op $UP_PD put 5000

	ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id "normal"

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
	if [ "$SINK_TYPE" == "kafka" ]; then
		stop_kafka_consumer
	fi
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
