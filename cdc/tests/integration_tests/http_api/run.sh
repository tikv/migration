#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
TLS_DIR=$(cd $CUR/../_certificates && pwd)
UP_PD=https://$UP_TLS_PD_HOST:$UP_TLS_PD_PORT
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT

function run() {
	pip3 install --user -U requests==2.26.0

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR --tikv-count 1
	start_tls_tidb_cluster --workdir $WORK_DIR --tlsdir $TLS_DIR

	cd $WORK_DIR

	echo " \
  [security]
   ca-path = \"$TLS_DIR/ca.pem\"
   cert-path = \"$TLS_DIR/server.pem\"
   key-path = \"$TLS_DIR/server-key.pem\"
   cert-allowed-cn = [\"fake_cn\"]
  " >$WORK_DIR/server.toml

	run_cdc_server \
		--workdir $WORK_DIR \
		--binary $CDC_BINARY \
		--logsuffix "_${TEST_NAME}_tls1" \
		--pd $UP_PD \
		--addr "127.0.0.1:8600" \
		--config "$WORK_DIR/server.toml" \
		--tlsdir "$TLS_DIR" \
		--cert-allowed-cn "client" # The common name of client.pem

	sleep 2

	run_cdc_server \
		--workdir $WORK_DIR \
		--binary $CDC_BINARY \
		--logsuffix "_${TEST_NAME}_tls2" \
		--pd $UP_PD \
		--addr "127.0.0.1:8601" \
		--config "$WORK_DIR/server.toml" \
		--tlsdir "$TLS_DIR" \
		--cert-allowed-cn "client" # The common name of client.pem

	# wait for cdc run
	sleep 2

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	python3 $CUR/util/test_case.py check_health $TLS_DIR
	python3 $CUR/util/test_case.py get_status $TLS_DIR

	python3 $CUR/util/test_case.py create_changefeed $TLS_DIR "$SINK_URI"
	# wait for changefeed created
	sleep 2

	# test processor query with no attached keyspans
	python3 $CUR/util/test_case.py get_processor $TLS_DIR

	rawkv_op $UP_PD put 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 5000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	sequential_cases=(
		"list_changefeed"
		"get_changefeed"
		"pause_changefeed"
		"update_changefeed"
		"resume_changefeed"
		"rebalance_keyspan"
		"list_processor"
		"get_processor"
		"move_keyspan"
		"set_log_level"
		"remove_changefeed"
		"resign_owner"
	)

	for case in ${sequential_cases[@]}; do
		python3 $CUR/util/test_case.py "$case" $TLS_DIR
	done

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
