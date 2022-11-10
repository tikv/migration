#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT
CF_ID="disk-full"

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	cat - >"$WORK_DIR/tikv-cdc-config.toml" <<EOF
per-changefeed-memory-quota=10485760 #10M
[sorter]
max-memory-consumption=0
EOF
	start_ts=$(get_start_ts $UP_PD)
	export GO_FAILPOINTS='github.com/tikv/migration/cdc/cdc/sorter/unified/SorterFlushDiskFull=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --config $WORK_DIR/tikv-cdc-config.toml

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	tikv-cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id=$CF_ID

	rawkv_op $UP_PD put 5000

	local max_retry=10
	local i
	for ((i = 0; i <= $max_retry; i++)); do
        state=$(tikv-cdc cli changefeed list --pd=$UP_PD | jq .[0]."summary" | jq ."state" | tr -d '"')
        echo "get changefeed state: $state with retry: $i"
        if [[ "$state" == "error" ]]; then
            break
        fi
		if [ "$i" -eq "$max_retry" ]; then
            echo "state is NOT expected to be normal"
			exit 1
		fi
        sleep 1
    done

	pid=$(pgrep -f "tikv-cdc" | head -n1)
	kill -9 $pid
	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --config $WORK_DIR/tikv-cdc-config.toml
	sleep 10

	tikv-cdc cli changefeed resume --pd=$UP_PD --changefeed-id=$CF_ID

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
