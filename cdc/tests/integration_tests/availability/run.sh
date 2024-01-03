#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
source $CUR/owner.sh
source $CUR/capture.sh
source $CUR/processor.sh
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1

export DOWN_TIDB_HOST
export DOWN_TIDB_PORT

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	start_ts=$(get_start_ts $UP_PD)

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	kafka) SINK_URI=$(get_kafka_sink_uri "$TEST_NAME") ;;
	*) SINK_URI="" ;;
	esac

	run_cdc_cli changefeed create \
		--start-ts=$start_ts --sink-uri="$SINK_URI" \
		--disable-version-check
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer --workdir "$WORK_DIR" --upstream-uri "$SINK_URI"
	fi
}

function cleanup() {
	if [ "$SINK_TYPE" == "kafka" ]; then
		stop_kafka_consumer
	fi
}

trap stop_tidb_cluster EXIT
prepare $*

test_owner_ha $*
test_capture_ha $*
test_processor_ha $*

cleanup
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
