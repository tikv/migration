#!/bin/bash

set -eu

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

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	sleep 10
	rawkv_op $UP_PD put 10000

	export GO_FAILPOINTS='github.com/tikv/migration/cdc/cdc/processor/processorManagerHandleNewChangefeedDelay=sleep(2000)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8600" --pd $UP_PD
	changefeed_id=$(tikv-cdc cli changefeed create --pd=$UP_PD --start-ts=$start_ts --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	# wait task is dispatched
	sleep 1

	capture_key=$(ETCDCTL_API=3 etcdctl get /tikv/cdc/capture --prefix | head -n 1)
	lease=$(ETCDCTL_API=3 etcdctl get $capture_key -w json | grep -o 'lease":[0-9]*' | awk -F: '{print $2}')
	lease_hex=$(printf '%x\n' $lease)
	# revoke lease of etcd capture key to simulate etcd session done
	ETCDCTL_API=3 etcdctl lease revoke $lease_hex

	sleep 1
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
