#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT

Start_Key=indexInfo_:_pf01_:_APD0101_:_0000000000000000000
SplitKey1=indexInfo_:_pf01_:_APD0101_:_0000000000000003000
SplitKey2=indexInfo_:_pf01_:_APD0101_:_0000000000000006000
End_Key=indexInfo_:_pf01_:_APD0101_:_0000000000000010000

CDC_COUNT=3

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	sleep 10

	rawkv_op $UP_PD put 10000

	# start $CDC_COUNT cdc servers, and create a changefeed
	for i in $(seq $CDC_COUNT); do
		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "$i" --addr "127.0.0.1:860${i}"
	done

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --format="raw" --start-key="$Start_Key" --end-key="$SplitKey1"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --format="raw" --start-key="$SplitKey1" --end-key="$SplitKey2"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --format="raw" --start-key="$SplitKey2" --end-key="$End_Key"

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
