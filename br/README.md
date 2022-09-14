# TiKV-BR

[![Build Status](https://github.com/tikv/migration/actions/workflows/ci-br.yml/badge.svg)](https://github.com/tikv/migration/actions/workflows/ci-br.yml)
[![codecov](https://codecov.io/gh/tikv/migration/branch/main/graph/badge.svg?token=7nmbrqKeWs)](https://codecov.io/gh/tikv/migration)
[![LICENSE](https://img.shields.io/github/license/tikv/migration)](https://github.com/tikv/migration/blob/main/br/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/tikv/migration/br)](https://goreportcard.com/report/github.com/tikv/migration/br)

**TiKV Backup & Restore (TiKV-BR)** is a command-line tool for distributed backup and restoration of the TiKV cluster data.

## Architecture

<img src="docs/images/tikv-br-architecture.png?sanitize=true" alt="architecture" width="600"/>

## Documentation

*TODO: Add documents link*

## Building

To build binary and run test:

```bash
$ make build   // build the binary with debug info
$ make release // build the release binary used for production
$ make test    // run unit test
```

*Notice TiKV-BR supports building with Go version `Go >= 1.18`*

When TiKV-BR is built successfully, you can find binary in the `bin` directory.

## Quick Start

```bash
# Using tiup to start a TiKV cluster and record the PD_ADDR
tiup playground --db 0 --pd 1 --kv 3 --monitor

# Using go-ycsb to generate test data.
git clone git@github.com:pingcap/go-ycsb.git
cd go-ycsb; make
./bin/go-ycsb load tikv -P workloads/workloada -p tikv.pd="${PD_ADDR}:2379" -p tikv.type="raw" -p recordcount=100000 -p operationcount=100000 --threads 100

# Backup ycsb test data.
bin/tikv-br backup raw \
	-s s3://backup-data/2022-09-16/_test/ \
	--pd ${PD_ADDR}:2379 \
	--log-file backup_test.log \

# Restore from the backup.
bin/br restore raw \
	-s s3://backup-data/2022-09-16/_test/ \
	--pd ${PD_ADDR}:2379 \
	--log-file restore_test.log
```

## Deploy 

### Recommended Deployment Configuration
- In production environments, deploy `TiKV-BR` on a node with at least 8 cores CPU and 16 GB memory. Select an appropriate OS version by following [Linux OS version requirements](https://docs.pingcap.com/tidb/dev/hardware-and-software-requirements#linux-os-version-requirements).

- Save backup data to Amazon S3 or one mounted network disk on all `TiKV-BR` and `TiKV` nodes.

- Allocate sufficient resources for backup and restoration.

TiKV-BR, TiKV nodes, and the backup storage system should provide network bandwidth that is greater than the backup speed. If the target cluster is particularly large, the threshold of backup and restoration speed is limited by the bandwidth of the backup network.
The backup storage system should also provide sufficient write/read performance (IOPS). Otherwise, the IOPS might become a performance bottleneck during backup or restoration.
TiKV nodes need to have at least two additional CPU cores and high performance disks for backups. Otherwise, the backup might have an impact on the services running on the cluster.

### Best practice
The following are some recommended operations for using `TiKV-BR` for backup and restoration:
- It is recommended that you perform the backup operation during off-peak hours to minimize the impact on applications.
- `TiKV-BR` supports restore on clusters of different topologies. However, the online applications will be greatly impacted during the restore operation. It is recommended that you perform restore during the off-peak hours or use rate-limit to limit the rate.
- It is recommended that you execute multiple backup operations serially. Running different backup operations in parallel reduces backup performance and also affects the online application.
- It is recommended that you execute multiple restore operations serially. Running different restore operations in parallel increases Region conflicts and also reduces restore performance.
- `TiKV-BR` supports checksum between `TiKV` cluster and backup files after backup or restore with the config `--checksum=true`. Note that, if checksum is enabled, please make sure no data is changed or `TTL` expired in `TiKV` cluster during backup or restore.
- TiKV-BR supports [`api-version`](https://docs.pingcap.com/zh/tidb/stable/tikv-configuration-file#api-version-%E4%BB%8E-v610-%E7%89%88%E6%9C%AC%E5%BC%80%E5%A7%8B%E5%BC%95%E5%85%A5) conversion from V1 to V2 with config `--dst-api-version V2`. Then restore the backup files to APIV2 `TiKV` cluster

### TiKV-BR Command Line Description
A br command consists of sub-commands, options, and parameters.

- Sub-command: the characters without - or --, including `backup`, `restore`, `raw` and `help`.
- Option: the characters that start with - or --.
- Parameter: the characters that immediately follow behind and are passed to the sub-command or the option.
#### Backup Raw Data
To back up the cluster raw data, use the `tikv-br backup raw` command. To get help on this command, execute `tikv-br backup raw -h` or `tikv-br backup raw --help`.
For example, backup raw data in TiKV cluster to `/tmp/backup` directory.

```
tikv-br backup raw \
    --pd "&{PDIP}:2379" \
    -s "s3://backup-data/2022-09-16/" \
    --s3.endpoint "s3://backup-data/2022-09-16/" \
    --dst-api-version v2 \
    --log-file="/tmp/br_backup.log
```
Explanations for some options in the above command are as follows:: 
- `backup`: Sub-command of `tikv-br`.
- `raw`: Sub-command of `backup`.
- `-s` or `--storage`: Storage of backup files.
- `"s3://backup-data/2022-09-16/"`: Parameter of `-s`, save the backup files in `"s3://backup-data/2022-09-16/"`.
- `--pd`: Service address of `PD`.
- `"${PDIP}:2379"`:  Parameter of `--pd`.
- `--dst-api-version`: The `api-version`, 请见 [tikv-server config](https://docs.pingcap.com/zh/tidb/stable/tikv-configuration-file#api-version-%E4%BB%8E-v610-%E7%89%88%E6%9C%AC%E5%BC%80%E5%A7%8B%E5%BC%95%E5%85%A5) of backup files.
- `v2`: Parameter of `--dst-api-version`, the optionals are `v1`, `v1ttl`, `v2`(Case insensitive). If no `dst-api-version` is specified, the `api-version` is the same with TiKV cluster of `--pd`.
A progress bar is displayed in the terminal during the backup. When the progress bar advances to 100%, the backup is complete. The progress bar is displayed as follows:
```
br backup raw \
    --pd "${PDIP}:2379" \
    --storage "s3://backup-data/2022-09-16/" \
    --log-file backupfull.log
Backup Raw <---------/................................................> 17.12%.
```

TiKV-BR can do checksum between TiKV cluster and backup files after backup finish with the config  `--checksum=true`.  
*Note: Please make sure no data is changed or `TTL` expired in `TiKV` cluster during backup.*

#### Restore Raw Data

To restore raw data to the cluster, execute the `tikv-br restore raw` command. To get help on this command, execute `tikv-br restore raw -h` or `tikv-br restore raw --help`.
For example, restore the raw backup files in `/tmp/backup` to `TiKV` cluster.

```
tikv-br restore raw \
    --pd "${PDIP}:2379" \
    --storage "s3://backup-data/2022-09-16/" \
    --ratelimit 128 \
    --log-file restoreraw.log
```
Explanations for some options in the above command are as follows:

- `--ratelimit`: specifies the maximum speed at which a restoration operation is performed (MiB/s) on each `TiKV` node.
- --log-file: specifies writing the TiKV-BR log to the `restorefull.log` file.
A progress bar is displayed in the terminal during the restoration. When the progress bar advances to 100%, the restoration is complete. The progress bar is displayed as follows:
```
tikv-br restore raw \
    --pd "${PDIP}:2379" \
    --storage "s3://backup-data/2022-09-16/" \
    --ratelimit 128 \
    --log-file restoreraw.log
Restore Raw <---------/...............................................> 17.12%.
```

TiKV-BR can do checksum between TiKV cluster and backup files after restoration finish with the config  `--checksum=true`.  
*Note: Please make sure no data is changed or `TTL` expired in `TiKV` cluster during backup.*

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

TiKV-BR is under the Apache 2.0 license. See the [LICENSE](./LICENSE.md) file for details.
