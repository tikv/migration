#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if [[ $# -eq 1 ]]; then
	# TODO: remove this branch when CI pipeline is updated.
	sink_type=tikv
	group=$1
elif [[ $# -eq 2 ]]; then
	sink_type=$1
	group=$2
else
	echo "Usage: $0 [sink_type] group"
	exit 1
fi

# Define groups
# Note: If new group is added, the group name must also be added to CI
# https://github.com/PingCAP-QE/ci/blob/main/pipelines/tikv/migration/latest/pull_integration_test.groovy
declare -A groups
groups=(
	["G00"]='autorandom kv_filter'
	["G01"]='capture_session_done_during_task cdc_hang_on'
	["G02"]='changefeed_auto_stop changefeed_error changefeed_fast_fail'
	["G03"]='changefeed_finish changefeed_pause_resume changefeed_reconstruct'
	["G04"]='cli tls http_api http_proxies'
	["G05"]='disk_full flow_control'
	["G06"]='gc_safepoint kill_owner'
	["G07"]='kv_client_stream_reconnect multi_capture'
	["G08"]='processor_err_chan processor_panic'
	["G09"]='processor_resolved_ts_fallback processor_stop_delay'
	["G10"]='sink_hang sigstop'
	["G11"]='sorter stop_downstream'
	["G12"]='availability' # heavy test case
)

# Get other cases not in groups, to avoid missing any case
others=()
for script in "$CUR"/*/run.sh; do
	test_name="$(basename "$(dirname "$script")")"
	# shellcheck disable=SC2076
	if [[ ! " ${groups[*]} " =~ " ${test_name} " ]]; then
		others=("${others[@]} ${test_name}")
	fi
done

# Get test names
test_names=""
# shellcheck disable=SC2076
if [[ "$group" == "others" ]]; then
	test_names="${others[*]}"
elif [[ " ${!groups[*]} " =~ " ${group} " ]]; then
	test_names="${groups[${group}]}"
else
	echo "Error: invalid group name: ${group}"
	exit 1
fi

# Run test cases
if [[ -n $test_names ]]; then
	echo "Run cases: ${test_names}"
	"${CUR}"/run.sh "${sink_type}" "${test_names}"
fi
