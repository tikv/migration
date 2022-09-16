#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT

MAX_RETRIES=50

function check_result() {
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
}

function test_capture_ha() {
	test_kill_capture
	test_hang_up_capture
	test_expire_capture
	check_result
}

# test_kill_capture starts two servers and kills the working one
# We expect the task is rebalanced to the live capture and the data
# continues to replicate.
function test_kill_capture() {
	echo "run test case test_kill_capture"
	# start one server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_kill_capture.server1

	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\"is-owner\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id

	rawkv_op $UP_PD put 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	# start the second capture
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8601" --logsuffix test_kill_capture.server2
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep -v \"$owner_id\" | grep id"
	capture_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}' | grep -v "$owner_id")

	# kill the owner
	kill -9 $owner_pid

	rawkv_op $UP_PD put 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
}

# test_hang_up_caputre starts two captures and hang up the working one by
# send SIGSTOP signal to the process.
# We expect the task is rebalanced to the live capture and the data continues
# to replicate.
function test_hang_up_capture() {
	echo "run test case test_hang_up_capture"
	# start one server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_hang_up_capture.server1

	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\"is-owner\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id

	# start the second capture
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8601" --logsuffix test_hang_up_capture.server2
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep -v \"$owner_id\" | grep id"
	capture_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}' | grep -v "$owner_id")

	kill -STOP $owner_pid
	rawkv_op $UP_PD put 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	kill -CONT $owner_pid
	cleanup_process $CDC_BINARY
}

# test_expire_capture start one server and then stop it unitl
# the session expires, and then resume the server.
# We expect the capture suicides itself and then recovers. The data
# should be replicated after recovering.
function test_expire_capture() {
	echo "run test case test_expire_capture"
	# start one server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\"is-owner\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id

	# stop the owner
	kill -SIGSTOP $owner_pid
	echo "process status:" $(ps -h -p $owner_pid -o "s")

	# ensure the session has expired
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\[\]'"

	# resume the owner
	kill -SIGCONT $owner_pid
	echo "process status:" $(ps -h -p $owner_pid -o "s")

	rawkv_op $UP_PD put 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
	rawkv_op $UP_PD delete 10000
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
}
