#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
# Here we don't use tikv-cdc.test, because the memory it uses is hard to calculate
CDC_BINARY=tikv-cdc
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	start_ts=$(get_start_ts $UP_PD)
	go-ycsb load tikv -P $CUR/config/workload -p tikv.pd="$UP_PD" -p tikv.type="raw" -p tikv.apiversion=V2 --threads 200 # About 500MiB

	cat - >"$WORK_DIR/tikv-cdc-config.toml" <<EOF
per-changefeed-memory-quota=10485760 #10M
[sorter]
max-memory-consumption=0
EOF

	export GO_FAILPOINTS='github.com/tikv/migration/cdc/cdc/processor/pipeline/ProcessorSinkFlushNothing=1200*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --config $WORK_DIR/tikv-cdc-config.toml
	rss0=$(ps -aux | grep 'tikv-cdc' | head -n1 | awk '{print $6}')
	if [[ $rss0 == "" ]]; then
		echo "Failed to get rrs0 by ps"
		exit 1
	fi

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	kafka) SINK_URI=$(get_kafka_sink_uri "$TEST_NAME") ;;
	*) SINK_URI="" ;;
	esac

	tikv-cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer --workdir "$WORK_DIR" --upstream-uri "$SINK_URI"
	fi

	# Wait until cdc pulls the data from tikv and store it in soter
	sleep 90

	rss1=$(ps -aux | grep 'tikv-cdc' | grep -v grep | head -n1 | awk '{print $6}')
	if [[ $rss1 == "" ]]; then
		echo "Failed to get rrs1 by ps"
		exit 1
	fi
	# We set `per-changefeed-memory-quota=10M` and forbid sorter to use memory cache data,
	# so maybe there is 10M of memory for data.
	# Note that there is memory usage between puller & sorter, and it's limited by size of channels.
	# Use small record size to reduce memory usage of this part (see flow_control/config/workload).
	expected=307200 #300M
	used=$(expr $rss1 - $rss0)
	echo "cdc server used memory: $used"
	if [ $used -gt $expected ]; then
		echo "Maybe flow-contorl is not working"
		# CI only collect *.log files, so name it as heap-dump.log
		curl http://127.0.0.1:8600/debug/pprof/heap >$WORK_DIR/heap-dump.log
		exit 1
	fi

	# As "per-changefeed-memory-quota" is low the syncing will cost more time.
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD 200

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
	if [ "$SINK_TYPE" == "kafka" ]; then
		stop_kafka_consumer
	fi
}

trap 'on_exit $? $LINENO $SINK_TYPE $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
