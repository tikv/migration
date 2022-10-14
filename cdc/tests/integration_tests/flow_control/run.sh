#!/bin/bash

set -euo pipefail

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

	start_ts=$(tikv-cdc cli tso query --pd=$UP_PD)
	sleep 10

	cat - >"$WORK_DIR/tikv-cdc-config.toml" <<EOF
per-changefeed-memory-quota=102400 #100K
[sorter]
max-memory-consumption=0
EOF

	export GO_FAILPOINTS='github.com/tikv/migration/cdc/cdc/processor/pipeline/ProcessorSinkFlushNothing=1000*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --config $WORK_DIR/tikv-cdc-config.toml
	rss0=$(ps -aux | grep 'tikv-cdc' | head -n1 | awk '{print $6}')
	if [[ $rss0 == "" ]]; then
		exit 1
	fi

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	tikv-cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	rawkv_op $UP_PD put 100000 # About 30M

	rss1=$(ps -aux | grep 'tikv-cdc' | head -n1 | awk '{print $6}')
	if [[ $rss1 == "" ]]; then
		exit 1
	fi
	expected=1048576 # 1M
	used=$(expr $rss1 - $rss0)

	echo "cdc server used memory: $used"
	if [ $used -gt $expected ]; then
		echo "Maybe flow-contorl is not working"
		exit 1
	fi

	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 100000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
