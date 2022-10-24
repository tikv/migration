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

function check_changefeed_mark_normal() {
	endpoints=$1
	changefeedid=$2
	error_msg=$3
	echo $changefeedid
	info=$(tikv-cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
	echo "$info"
	state=$(echo $info | jq -r '.state')
	if [[ ! "$state" == "normal" ]]; then
		echo "changefeed state $state does not equal to normal"
		exit 1
	fi
	message=$(echo $info | jq -r '.error.message')
	if [[ ! "$message" =~ "$error_msg" ]]; then
		echo "error message '$message' is not as expected '$error_msg'"
		exit 1
	fi
}

export -f check_changefeed_mark_normal

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	export GO_FAILPOINTS='github.com/tikv/migration/cdc/cdc/processor/pipeline/ProcessorAddKeySpanError=1*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8600" --pd $pd_addr

	start_ts=$(tikv-cdc cli tso query --pd=$UP_PD)
	start_ts=$(expr $start_ts - $FALL_BACK)
	changefeed_id=$(tikv-cdc cli changefeed create --pd=$pd_addr --start_ts=$start_ts --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	retry_time=10
	ensure $retry_time check_changefeed_mark_normal $pd_addr $changefeed_id "null"

	rawkv_op $UP_PD put 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs_contains $WORK_DIR "processor add keyspan injected error"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
