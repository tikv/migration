#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT

MAX_RETRIES=20

function test_processor_ha() {
	test_stop_processor
}

# test_stop_processor stops the working processor
# and then resume it.
# We expect the data after resuming is replicated.
function test_stop_processor() {
	echo "run test case test_stop_processor"
	# start a capture server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_stop_processor
	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\"is-owner\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id

	# get the change feed id
	changefeed=$($CDC_BINARY cli changefeed list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}')
	echo "changefeed id:" $changefeed

	# stop the change feed job
	# use ensure to wait for the change feed loading into memory from etcd
	ensure $MAX_RETRIES "curl -s -d \"cf-id=$changefeed&admin-job=1\" http://127.0.0.1:8600/capture/owner/admin | grep true"

    rawkv_op $UP_PD put 10000

	# resume the change feed job
	curl -d "cf-id=$changefeed&admin-job=2" http://127.0.0.1:8600/capture/owner/admin
    check_sync_diff $WORK_DIR $UP_PD $DOWN_PD
    rawkv_op $UP_PD delete 10000
    check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	echo "test_stop_processor pass"
	cleanup_process $CDC_BINARY
}
