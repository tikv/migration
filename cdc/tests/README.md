## Preparations

### Run integration tests locally

Assume that you are in the root directory of source code (`github.com/tikv/migration/cdc`).

1. The following executables must be copied or generated or linked into these locations

   * `scripts/bin/tidb-server` # version >= 6.2.0
   * `scripts/bin/tikv-server` # version >= 6.2.0
   * `scripts/bin/pd-server`   # version >= 6.2.0
   * `scripts/bin/pd-ctl`      # version >= 6.2.0
   * [scripts/bin/go-ycsb](https://github.com/pingcap/go-ycsb)
   * [scripts/bin/etcdctl](https://github.com/etcd-io/etcd/tree/master/etcdctl)
   * [scripts/bin/jq](https://stedolan.github.io/jq/)

   > If you are running tests on Linux x86-64, you can run `make prepare_test_binaries` to get all necessary binaries.
   >
   > If you are running tests on MacOS, tidb related binaries can be downloaded from tiup mirrors, such as https://tiup-mirrors.pingcap.com/tidb-v6.2.0-darwin-amd64.tar.gz.

2. The user used to execute the tests must have permission to create the folder /tmp/tikv_cdc_test. All test artifacts
   will be written into this folder.

### Run integration tests in docker

The following programs must be installed:

* [docker](https://docs.docker.com/get-docker/)
* [docker-compose](https://docs.docker.com/compose/install/)

We recommend that you provide docker with at least 6+ cores and 8G+ memory. Of course, the more resources, the better.

## Running

### Unit Test

1. Unit test does not need any dependencies, just running `make unit_test` in root dir of source code, or `cd` into
   directory of a test case and run single case via `GO111MODULE=on go test -check.f TestXXX`.

### Integration Test

#### Run integration tests locally

1. Run `make integration_test_build` to generate `tikv-cdc` binaries for integration test

2. Run `make integration_test` to execute the integration tests. This command will

   1. Check that all required executables exist.
   2. Execute `tests/integration_tests/run.sh`

   > If want to run one integration test case only, just pass the CASE parameter, e.g. `make integration_test CASE=autorandom`.
   >
   > There are some environment variables that you can set by yourself, see [test_prepare](./integration_tests/_utils/test_prepare).

#### Run integration tests in docker

1. Run `tests/up.sh`. This script will setup a container with some tools ready, and run `/bin/bash` interactively in the container.

2. Run `make integration_test` or `make integration_test CASE=[test name]` to execute the integration tests.

> **Warning:**
> These scripts and files may not work under the arm architecture,
> and we have not tested against it.

Some useful tips:

- You can specify multiple tests to run in CASE, for example: `CASE="cli cli_tls"`. You can even
   use `CASE="*"` to indicate that you are running all tests。

## Writing new tests

1. Write new integration tests as shell scripts in `tests/integration_tests/TEST_NAME/run.sh`. The script should
exit with a nonzero error code on failure.

2. Add TEST_NAME to existing group in [run_group.sh](./integration_tests/run_group.sh), or add a new group for it.

3. If you add a new group, the name of the new group must be added to [CI](https://github.com/PingCAP-QE/ci/blob/main/pipelines/tikv/migration/latest/pull_integration_test.groovy). 
