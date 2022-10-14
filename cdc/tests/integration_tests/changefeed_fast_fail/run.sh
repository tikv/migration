#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT
MAX_RETRIES=20

function check_changefeed_mark_failed_regex() {
	endpoints=$1
	changefeedid=$2
	error_msg=$3
	info=$(tikv-cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
	echo "$info"
	state=$(echo $info | jq -r '.state')
	if [[ ! "$state" == "failed" ]]; then
		echo "changefeed state $state does not equal to failed"
		exit 1
	fi
	message=$(echo $info | jq -r '.error.message')
	if [[ ! "$message" =~ $error_msg ]]; then
		echo "error message '$message' does not match '$error_msg'"
		exit 1
	fi
}

export -f check_changefeed_mark_failed_regex

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	sleep 10
	rawkv_op $UP_PD put 10000
	export GO_FAILPOINTS='github.com/tikv/migration/cdc/cdc/owner/InjectChangefeedFastFailError=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	changefeedid="changefeed-fast-fail"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $changefeedid

	ensure $MAX_RETRIES check_changefeed_mark_failed_regex http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid} "ErrGCTTLExceeded"
	run_cdc_cli changefeed remove -c $changefeedid
	sleep 2
	result=$(tikv-cdc cli changefeed list)
	if [[ ! "$result" == "[]" ]]; then
		echo "changefeed remove failed"
		exit 1
	fi

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
