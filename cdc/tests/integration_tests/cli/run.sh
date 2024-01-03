#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT
TLS_DIR=$(cd $CUR/../_certificates && pwd)

function check_changefeed_state() {
	changefeedid=$1
	expected=$2
	output=$(tikv-cdc cli changefeed query --simple --changefeed-id $changefeedid --pd=$UP_PD 2>&1)
	state=$(echo $output | grep -oE "\"state\": \"[a-z]+\"" | tr -d '" ' | awk -F':' '{print $(NF)}')
	if [ "$state" != "$expected" ]; then
		echo "unexpected state $output, expected $expected"
		exit 1
	fi
}

function check_changefeed_count() {
	pd_addr=$1
	expected=$2
	feed_count=$(tikv-cdc cli changefeed list --pd=$pd_addr | jq '.|length')
	if [[ "$feed_count" != "$expected" ]]; then
		echo "[$(date)] <<<<< unexpect changefeed count! expect ${expected} got ${feed_count} >>>>>"
		exit 1
	fi
	echo "changefeed count ${feed_count} check pass, pd_addr: $pd_addr"
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR --multiple-upstream-pd true

	cd $WORK_DIR
	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"

	start_ts=$(get_start_ts $UP_PD)
	rawkv_op $UP_PD put 5000

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	kafka) SINK_URI=$(get_kafka_sink_uri "$TEST_NAME") ;;
	*) SINK_URI="" ;;
	esac

	uuid="custom-changefeed-name"
	run_cdc_cli changefeed create --start-ts=$start_ts --sort-engine=memory --sink-uri="$SINK_URI" --tz="Asia/Shanghai" -c="$uuid"
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "$SINK_URI"
	fi

	# Make sure changefeed is created.
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	check_changefeed_state $uuid "normal"
	check_changefeed_count http://${UP_PD_HOST_1}:${UP_PD_PORT_1} 1
	check_changefeed_count http://${UP_PD_HOST_2}:${UP_PD_PORT_2} 1
	check_changefeed_count http://${UP_PD_HOST_3}:${UP_PD_PORT_3} 1
	check_changefeed_count http://${UP_PD_HOST_1}:${UP_PD_PORT_1},http://${UP_PD_HOST_2}:${UP_PD_PORT_2},http://${UP_PD_HOST_3}:${UP_PD_PORT_3} 1

	# Make sure changefeed can not be created if the name is already exists.
	set +e
	exists=$(run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id="$uuid" 2>&1 | grep -oE 'already exists')
	set -e
	if [[ -z $exists ]]; then
		echo "[$(date)] <<<<< unexpect output got ${exists} >>>>>"
		exit 1
	fi

	# Update changefeed failed because changefeed is running
	cat - >"$WORK_DIR/changefeed.toml" <<EOF
EOF
	set +e
	update_result=$(tikv-cdc cli changefeed update --pd=$pd_addr --config="$WORK_DIR/changefeed.toml" --no-confirm --changefeed-id $uuid 2>&1)
	set -e
	if [[ ! $update_result == *"can only update changefeed config when it is stopped"* ]]; then
		echo "update changefeed config should fail when changefeed is running, got $update_result"
		exit 1
	fi

	# Pause changefeed
	run_cdc_cli changefeed --changefeed-id $uuid pause && sleep 3
	jobtype=$(run_cdc_cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
	if [[ $jobtype != 1 ]]; then
		echo "[$(date)] <<<<< unexpect admin job type! expect 1 got ${jobtype} >>>>>"
		exit 1
	fi
	check_changefeed_state $uuid "stopped"

	# Update changefeed
	run_cdc_cli changefeed update --pd=$pd_addr --config="$WORK_DIR/changefeed.toml" --no-confirm --changefeed-id $uuid
	changefeed_info=$(run_cdc_cli changefeed query --changefeed-id $uuid 2>&1)
	if [[ ! $changefeed_info == *"\"sort-engine\": \"memory\""* ]]; then
		echo "[$(date)] <<<<< changefeed info is not updated as expected ${changefeed_info} >>>>>"
		exit 1
	fi

	jobtype=$(run_cdc_cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
	if [[ $jobtype != 1 ]]; then
		echo "[$(date)] <<<<< unexpect admin job type! expect 1 got ${jobtype} >>>>>"
		exit 1
	fi

	# Resume changefeed
	run_cdc_cli changefeed --changefeed-id $uuid resume && sleep 3
	jobtype=$(run_cdc_cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
	if [[ $jobtype != 0 ]]; then
		echo "[$(date)] <<<<< unexpect admin job type! expect 0 got ${jobtype} >>>>>"
		exit 1
	fi
	check_changefeed_state $uuid "normal"

	# Remove changefeed
	run_cdc_cli changefeed --changefeed-id $uuid remove && sleep 3
	check_changefeed_count http://${UP_PD_HOST_1}:${UP_PD_PORT_1} 0

	run_cdc_cli changefeed create --sink-uri="$SINK_URI" --tz="Asia/Shanghai" -c="$uuid" && sleep 3
	check_changefeed_state $uuid "normal"

	# Make sure bad sink url fails at creating changefeed.
	set +e
	create_result=$(tikv-cdc cli changefeed create --start-ts=$start_ts --sink-uri="mysql://baksink" 2>&1)
	set -e
	if [[ ! $create_result == *"the sink scheme (mysql) is not supported"* ]]; then
		echo "<<<<< unexpect output got ${create_result} >>>>>"
		exit 1
	fi

	# Smoke test unsafe commands
	echo "y" | run_cdc_cli unsafe delete-service-gc-safepoint
	run_cdc_cli unsafe reset --no-confirm

	# Smoke test change log level
	curl -X POST -d '"warn"' http://127.0.0.1:8600/api/v1/log
	sleep 3
	# make sure TiKV-CDC does not panic
	curl http://127.0.0.1:8600/status

	cleanup_process $CDC_BINARY
	if [ "$SINK_TYPE" == "kafka" ]; then
		stop_kafka_consumer
	fi
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
