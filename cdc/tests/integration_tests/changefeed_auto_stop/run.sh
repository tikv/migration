#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT
SINK_TYPE=$1

function check_changefeed_state() {
	endpoints=$1
	changefeedid=$2
	expected=$3
	error_msg=$4
	info=$(tikv-cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
	echo "$info"
	state=$(echo $info | jq -r '.state')
	if [[ ! "$state" == "$expected" ]]; then
		echo "changefeed state $state does not equal to $expected"
		exit 1
	fi
	message=$(echo $info | jq -r '.error.message')
	if [[ ! "$message" =~ "$error_msg" ]]; then
		echo "error message '$message' is not as expected '$error_msg'"
		exit 1
	fi
}

export -f check_changefeed_state

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR
	start_ts=$(get_start_ts $UP_PD)
	# TODO: use go-ycsb to generate data?
	rawkv_op $UP_PD put 5000

	export GO_FAILPOINTS='github.com/tikv/migration/cdc/cdc/processor/pipeline/ProcessorSyncResolvedError=1*return(true);github.com/tikv/migration/cdc/cdc/processor/ProcessorUpdatePositionDelaying=sleep(1000)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8600" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	kafka) SINK_URI=$(get_kafka_sink_uri "$TEST_NAME") ;;
	*) SINK_URI="" ;;
	esac

	changefeedid=$(tikv-cdc cli changefeed create --pd="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" --start-ts=$start_ts --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer --workdir "$WORK_DIR" --upstream-uri "$SINK_URI"
	fi

	# make sure the first capture does job first.
	sleep 3
	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2" --addr "127.0.0.1:8601" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"

	ensure 10 check_changefeed_state $UP_PD ${changefeedid} "normal" "null"

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
	if [ "$SINK_TYPE" == "kafka" ]; then
		stop_kafka_consumer
	fi
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
# the "1" below is the log suffix
# only cdc1.log contain the error log "processor sync..."
check_logs_contains $WORK_DIR "processor sync resolved injected error" "1"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
