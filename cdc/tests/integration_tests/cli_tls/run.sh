#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
TLS_DIR=$(cd $CUR/../_certificates && pwd)
UP_PD=https://$UP_TLS_PD_HOST:$UP_TLS_PD_PORT
DOWN_PD=https://$DOWN_TLS_PD_HOST:$DOWN_TLS_PD_PORT
SUFFIX=" --pd=$UP_PD --ca=$TLS_DIR/ca.pem --cert=$TLS_DIR/client.pem --key=$TLS_DIR/client-key.pem"

function check_changefeed_state() {
	changefeedid=$1
	expected=$2
	output=$(tikv-cdc cli changefeed query --simple --changefeed-id $changefeedid $SUFFIX 2>&1)
	state=$(echo $output | grep -oE "\"state\": \"[a-z]+\"" | tr -d '" ' | awk -F':' '{print $(NF)}')
	if [ "$state" != "$expected" ]; then
		echo "unexpected state $output, expected $expected"
		exit 1
	fi
}

function check_count() {
	cmd=$1
	expected=$2
	count=$(tikv-cdc cli $cmd $SUFFIX | jq '.|length')
	if [[ "$count" != "$expected" ]]; then
		echo "[$(date)] <<<<< unexpect 'cli $cmd' count! expect ${expected} got ${count} >>>>>"
		exit 1
	fi
	echo "'cli $cmd' count ${count} check pass"
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

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

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_TLS_PD_HOST}:${DOWN_TLS_PD_PORT}/?ca-path=$TLS_DIR/ca.pem&cert-path=$TLS_DIR/client.pem&key-path=$TLS_DIR/client-key.pem" ;;
	*) SINK_URI="" ;;
	esac

	ID="feed01"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c=$ID $SUFFIX
	# TODO: optimize here
	sleep 10

	rawkv_op $UP_PD put 500
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	# changefeed
	check_changefeed_state $ID "normal"
	run_cdc_cli changefeed pause -c=$ID $SUFFIX
	sleep 2
	check_changefeed_state $ID "stopped"
	run_cdc_cli changefeed update -c=$ID --sort-engine="memory" --no-confirm $SUFFIX
	run_cdc_cli changefeed resume -c=$ID $SUFFIX
	sleep 2
	check_changefeed_state $ID "normal"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c="feed02" $SUFFIX
	sleep 2
	check_changefeed_state $ID "normal"
	check_count "changefeed list" 2
	run_cdc_cli changefeed remove -c="feed02" $SUFFIX
	sleep 2
	check_changefeed_state $ID "normal"
	check_count "changefeed list" 1

	# capture
	check_count "capture list" 1
	# processor
	check_count "processor list" 1
	capture=$(run_cdc_cli processor list $SUFFIX | grep 'capture_id' | awk '{print $2}' | tr -d '"')
	# We can get processor information by processor query as follow:
	# {
	#   "status": {...},
	#   "operation": {...},
	# }
	# There are two elements at the top level, so we should `check_cout 2`
	check_count "processor query -c=$ID -p=$capture" 2

	rawkv_op $UP_PD delete 500
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
