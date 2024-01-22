#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT
CF_ID="stop-downstream"

# Note for Kafka sink: "stop_downstream" stops PD & TiKV in downstream cluster, but not Kafka.
# TODO: stop Kafka in Kafka sink.
function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	start_ts=$(get_start_ts $UP_PD)
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	kafka) SINK_URI=$(get_kafka_sink_uri "$TEST_NAME") ;;
	*) SINK_URI="" ;;
	esac

	tikv-cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id=$CF_ID
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer --workdir "$WORK_DIR" --upstream-uri "$SINK_URI"
	fi

	rawkv_op $UP_PD put 5000

	down_pd_pid=$(ps -aux | grep "pd-server" | grep "down_pd" | awk '{print $2}' | head -n1)
	down_tikv_pid=$(ps -aux | grep "tikv-server" | grep "tikv_down" | awk '{print $2}' | head -n1)

	# stop downstream
	kill -19 $down_pd_pid
	kill -19 $down_tikv_pid

	# Wait for cdc to retry to create tikv sink
	# TODO: find better way to speed up the retry, now integration test takes too long.
	sleep 180

	# resume downstream
	kill -18 $down_pd_pid
	kill -18 $down_tikv_pid
	# make sure servers recover
	sleep 10

	state=$(tikv-cdc cli changefeed list --pd=$UP_PD | jq .[0]."summary" | jq ."state" | tr -d '"')
	if [[ "$state" == "error" ]]; then
		tikv-cdc cli changefeed resume --pd=$UP_PD --changefeed-id=$CF_ID
	fi

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
	if [ "$SINK_TYPE" == "kafka" ]; then
		stop_kafka_consumer
	fi
}

trap 'on_exit $? $LINENO $SINK_TYPE $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
