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

function check_changefeed_mark_error() {
	endpoints=$1
	changefeedid=$2
	error_msg=$3
	info=$(tikv-cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
	echo "$info"
	state=$(echo $info | jq -r '.state')
	if [[ ! "$state" == "error" ]]; then
		echo "changefeed state $state does not equal to error"
		exit 1
	fi
	message=$(echo $info | jq -r '.error.message')
	if [[ ! "$message" =~ "$error_msg" ]]; then
		echo "error message '$message' is not as expected '$error_msg'"
		exit 1
	fi
}

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

function check_changefeed_mark_stopped_regex() {
	endpoints=$1
	changefeedid=$2
	error_msg=$3
	info=$(tikv-cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
	echo "$info"
	state=$(echo $info | jq -r '.state')
	if [[ ! "$state" == "stopped" ]]; then
		echo "changefeed state $state does not equal to stopped"
		exit 1
	fi
	message=$(echo $info | jq -r '.error.message')
	if [[ ! "$message" =~ $error_msg ]]; then
		echo "error message '$message' does not match '$error_msg'"
		exit 1
	fi
}

function check_changefeed_mark_stopped() {
	endpoints=$1
	changefeedid=$2
	error_msg=$3
	info=$(tikv-cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
	echo "$info"
	state=$(echo $info | jq -r '.state')
	if [[ ! "$state" == "stopped" ]]; then
		echo "changefeed state $state does not equal to stopped"
		exit 1
	fi
	message=$(echo $info | jq -r '.error.message')
	if [[ ! "$message" =~ "$error_msg" ]]; then
		echo "error message '$message' is not as expected '$error_msg'"
		exit 1
	fi
}

function check_no_changefeed() {
	pd=$1
	count=$(tikv-cdc cli changefeed list --pd=$pd 2>&1 | jq '.|length')
	if [[ ! "$count" -eq "0" ]]; then
		exit 1
	fi
}

export -f check_changefeed_mark_error
export -f check_changefeed_mark_failed_regex
export -f check_changefeed_mark_stopped_regex
export -f check_changefeed_mark_stopped
export -f check_no_changefeed

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	start_ts=$(get_start_ts $UP_PD)
	# TODO: use go-ycsb to generate data?
	rawkv_op $UP_PD put 5000

	export GO_FAILPOINTS='github.com/tikv/migration/cdc/cdc/owner/NewChangefeedNoRetryError=1*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	capture_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	changefeedid="changefeed-error"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $changefeedid

	ensure $MAX_RETRIES check_changefeed_mark_failed_regex $UP_PD ${changefeedid} ".*CDC:ErrStartTsBeforeGC.*"
	run_cdc_cli changefeed resume -c $changefeedid

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	export GO_FAILPOINTS='github.com/tikv/migration/cdc/cdc/owner/NewChangefeedRetryError=return(true)'
	kill $capture_pid
	check_count 0 "tikv-cdc" $UP_PD $MAX_RETRIES
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	ensure $MAX_RETRIES check_changefeed_mark_error $UP_PD ${changefeedid} "failpoint injected retriable error"

	run_cdc_cli changefeed remove -c $changefeedid
	ensure $MAX_RETRIES check_no_changefeed $UP_PD

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY

	# updating GC safepoint failure case
	export GO_FAILPOINTS='github.com/tikv/migration/cdc/pkg/txnutil/gc/InjectActualGCSafePoint=return(9223372036854775807)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	changefeedid_1="changefeed-error-1"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $changefeedid_1
	ensure $MAX_RETRIES check_changefeed_mark_failed_regex $UP_PD ${changefeedid_1} "[CDC:ErrStartTsBeforeGC]"

	run_cdc_cli changefeed remove -c $changefeedid_1
	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
