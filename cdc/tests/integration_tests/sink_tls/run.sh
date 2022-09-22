#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
TLS_DIR=$(cd $CUR/../_certificates && pwd)
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=https://$TLS_PD_HOST:$TLS_PD_PORT

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR
	start_tls_tidb_cluster --workdir $WORK_DIR --tlsdir $TLS_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(tikv-cdc cli tso query --pd=$UP_PD)
	sleep 10
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${TLS_PD_HOST}:${TLS_PD_PORT}/?ca-path=$TLS_DIR/ca.pem&cert-path=$TLS_DIR/client.pem&key-path=$TLS_DIR/client-key.pem" ;;
	*) SINK_URI="" ;;
	esac

	tikv-cdc cli changefeed create --start-ts=$start_ts --sink-uri=$SINK_URI

	rawkv_op $UP_PD put 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
