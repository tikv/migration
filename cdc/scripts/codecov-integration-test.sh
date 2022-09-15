#!/bin/bash

curl -Os https://uploader.codecov.io/latest/linux/codecov
chmod +x codecov
./codecov -F cdc-integration-test -s /tmp/tikv_cdc_test/tests/tiup-cluster/cover -f '*.out' -t $TIKV_MIGRATION_CODECOV_TOKEN
rm -rf codecov
