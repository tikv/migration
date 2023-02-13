#!/bin/bash

set -eux

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=tikv-cdc.test
SINK_TYPE=$1
UP_PD=http://$UP_PD_HOST_1:$UP_PD_PORT_1,http://$UP_PD_HOST_2:$UP_PD_PORT_2,http://$UP_PD_HOST_3:$UP_PD_PORT_3
DOWN_PD=http://$DOWN_PD_HOST:$DOWN_PD_PORT

function run_kill_upstream() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR --multiple-upstream-pd "true"
	cd $WORK_DIR

	start_ts=$(get_start_ts $UP_PD)
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8600" --pd "$UP_PD"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2" --addr "127.0.0.1:8601" --pd "$UP_PD"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "3" --addr "127.0.0.1:8602" --pd "$UP_PD"

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${DOWN_PD_HOST}:${DOWN_PD_PORT}" ;;
	*) SINK_URI="" ;;
	esac

	tikv-cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"

	rawkv_op $UP_PD put 10000 &
	sleep 1
	# send sigstop to tikv
	n=$(echo $(($RANDOM % 3 + 1)))
	tikv_pid=$(pgrep -f "tikv$n" | head -n1)
	kill -19 $tikv_pid
	sleep 10
	check_count 2 "tikv" $UP_PD

	kill -18 $tikv_pid
	check_count 3 "tikv" $UP_PD
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	# Ignore the test on PD, because sending SIGSTOP to PD may cause CDC to exit,
	# `cdc_hang_on` has tested sending SIGSTOP to PD leader.

	rawkv_op $UP_PD delete 10000 &
	sleep 1
	# send sigstop to tikv-cdc
	n=$(echo $(($RANDOM % 2 + 1)))
	cdc_pid=$(pgrep -f "tikv-cdc" | sed -n "$n"p)
	kill -19 $cdc_pid
	sleep 10
	check_count 2 "tikv-cdc" $UP_PD

	kill -18 $cdc_pid
	check_count 3 "tikv-cdc" $UP_PD
	check_sync_diff $WORK_DIR $UP_PD $DOWN_PD

	cleanup_process $CDC_BINARY
}

function run_kill_downstream() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR --multiple-upstream-pd "true"
	cd $WORK_DIR

	# We start 3 tikv and 3 pd in cluster1(usually as upstream),
	# 1 tikv and 1 pd in cluster2(usually as downstream).
	# Now we treat cluster1 as the downstream cluster and cluster2 as upstream,
	# so we can ensure high availability of downstream clusters while sending SIGSTOP.

	start_ts=$(get_start_ts $DOWN_PD)
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8600" --pd "$DOWN_PD"

	case $SINK_TYPE in
	tikv) SINK_URI="tikv://${UP_PD_HOST_1}:${UP_PD_PORT_1}" ;;
	*) SINK_URI="" ;;
	esac

	tikv-cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --pd $DOWN_PD

	rawkv_op $DOWN_PD put 10000 &
	sleep 1
	# send sigstop to tikv
	n=$(echo $(($RANDOM % 3 + 1)))
	tikv_pid=$(pgrep -f "tikv$n" | head -n1)
	kill -19 $tikv_pid
	sleep 10
	check_count 2 "tikv" $UP_PD

	kill -18 $tikv_pid
	check_count 3 "tikv" $UP_PD
	check_sync_diff $WORK_DIR $DOWN_PD $UP_PD

	rawkv_op $DOWN_PD delete 10000 &
	sleep 1
	# send sigstop to pd
	n=$(echo $(($RANDOM % 3 + 1)))
	pd_pid=$(pgrep -f "pd-server" | sed -n "$n"p)
	kill -19 $pd_pid
	sleep 10
	# PD would not recover when ETCD leader is stopped. So skip check_count here.
	# check_count 2 "pd" $UP_PD

	kill -18 $pd_pid
	check_count 3 "pd" $UP_PD
	check_sync_diff $WORK_DIR $DOWN_PD $UP_PD

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run_kill_upstream $*
run_kill_downstream $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
