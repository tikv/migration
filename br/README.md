# TiKV-BR

[![Build Status](https://github.com/tikv/migration/actions/workflows/ci-br.yml/badge.svg)](https://github.com/tikv/migration/actions/workflows/ci-br.yml)
[![codecov](https://codecov.io/gh/tikv/migration/branch/main/graph/badge.svg?token=7nmbrqKeWs&flag=br)](https://app.codecov.io/gh/tikv/migration/tree/main/br)
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

*Notice TiKV-BR requires building with Go version `Go >= 1.18`*

When TiKV-BR is built successfully, you can find binary in the `bin` directory.

## Quick Start

```bash
# Using tiup to start a TiKV cluster and record the PD_ADDR
tiup playground --db 0 --pd 1 --kv 3 --monitor

# Using go-ycsb to generate test data.
git clone git@github.com:pingcap/go-ycsb.git
cd go-ycsb; make
./bin/go-ycsb load tikv -P workloads/workloada -p tikv.pd="${PD_ADDR}:2379" \
    -p tikv.type="raw" -p recordcount=100000 -p operationcount=100000 --threads 100

# Backup ycsb test data.
bin/tikv-br backup raw \
	-s s3://backup-data/2022-09-16/_test/ \
	--pd ${PD_ADDR}:2379 \
	--log-file backup_test.log \

# Restore from the backup.
bin/tikv-br restore raw \
	-s s3://backup-data/2022-09-16/_test/ \
	--pd ${PD_ADDR}:2379 \
	--log-file restore_test.log
```

## Deploy 

### Recommended Deployment Configuration
- In production environments, deploy `TiKV-BR` on a node with at least 8 cores CPU and 16 GB memory. Select an appropriate OS version by following [Linux OS version requirements](https://docs.pingcap.com/tidb/dev/hardware-and-software-requirements#linux-os-version-requirements).

- Save backup data to Amazon S3 or one mounted network disk on all `TiKV-BR` and `TiKV` nodes.

- Allocate sufficient resources for backup and restoration.

- TiKV-BR only support raw data backup/restoration in TiKV cluster with version >= `6.1.0`. May support TiKV cluster with 5.x versions in the future.

TiKV-BR, TiKV nodes, and the backup storage system should provide network bandwidth that is greater than the backup speed. If the target cluster is particularly large, the threshold of backup and restoration speed is limited by the bandwidth of the backup network.  
The backup storage system should also provide sufficient write/read performance (IOPS). Otherwise, the IOPS might become a performance bottleneck during backup or restoration.  
TiKV nodes need to have at least two additional spared CPU cores and disk bandwidth (related to `ratelimit` parameter) for backups. Otherwise, the backup might have an impact on the services running on the cluster.

### Best practice
The following are some recommended operations for using `TiKV-BR` for backup and restoration:
- It is recommended that you perform the backup operation during off-peak hours to minimize the impact on applications.
- `TiKV-BR` supports restore on clusters of different topologies. However, the online applications will be greatly impacted during the restore operation. It is recommended that you perform restore during the off-peak hours or use `ratelimit` to limit the rate.
- It is recommended that you execute multiple backup operations serially. Running different backup operations in parallel reduces backup performance and also affects the online application.
- It is recommended that you execute multiple restore operations serially. Running different restore operations in parallel increases Region conflicts and also reduces restore performance.
- `TiKV-BR` supports checksum between `TiKV` cluster and backup files after backup or restore with the config `--checksum=true`. Note that, if checksum is enabled, please make sure no data is changed or `TTL` expired in `TiKV` cluster during backup or restore.
- TiKV-BR supports [`api-version`](https://docs.pingcap.com/tidb/stable/tikv-configuration-file#api-version-new-in-v610) conversion from V1 to V2 with config `--dst-api-version V2`. Then restore the backup files to APIV2 `TiKV` cluster. This is mainly used to upgrade from APIV1 cluster to APIV2 cluster.

### TiKV-BR Command Line Description
A tikv-br command consists of sub-commands, options, and parameters.

- Sub-command: the characters without - or --, including `backup`, `restore`, `raw` and `help`.
- Option: the characters that start with - or --.
- Parameter: the characters that immediately follow behind and are passed to the sub-command or the option.
#### Backup Raw Data
To back up the cluster raw data, use the `tikv-br backup raw` command. To get help on this command, execute `tikv-br backup raw -h` or `tikv-br backup raw --help`.
For example, backup raw data in TiKV cluster to s3 `/backup-data/2022-09-16` directory.

```
export AWS_ACCESS_KEY_ID=&{AWS_KEY_ID};
export AWS_SECRET_ACCESS_KEY=&{AWS_KEY};
tikv-br backup raw \
    --pd "&{PDIP}:2379" \
    -s "s3://backup-data/2022-09-16/" \
    --ratelimit 128 \
    --dst-api-version v2 \
    --log-file="/tmp/br_backup.log \
    --gcttl=5m \
    --start="a" \
    --end="z" \
    --format="raw"
```
Explanations for some options in the above command are as follows: 
- `backup`: Sub-command of `tikv-br`.
- `raw`: Sub-command of `backup`.
- `-s` or `--storage`: Storage of backup files.
- `"s3://backup-data/2022-09-16/"`: Parameter of `-s`, save the backup files in `"s3://backup-data/2022-09-16/"`.
- `--ratelimit`: The maximum speed at which a backup operation is performed on each `TiKV` node.
- `128`: The value of `ratelimit`, unit is MiB/s.
- `--pd`: Service address of `PD`.
- `"${PDIP}:2379"`:  Parameter of `--pd`.
- `--dst-api-version`: The `api-version`, please see [tikv-server config](https://docs.pingcap.com/tidb/stable/tikv-configuration-file#api-version-new-in-v610).
- `v2`: Parameter of `--dst-api-version`, the optionals are `v1`, `v1ttl`, `v2`(Case insensitive). If no `dst-api-version` is specified, the `api-version` is the same with TiKV cluster of `--pd`.
- `gcttl`: The pause duration of GC. This can be used to make sure that the incremental data from backup start to TiKV-CDC [create changefeed](https://github.com/tikv/migration/blob/main/cdc/README.md#create-a-replication-task) will NOT be deleted by GC. 5 minutes by default.
- `5m`: Paramater of `gcttl`. Its format is `number + unit`, e.g. `24h` means 24 hours, `60m` means 60 minutes.
- `start`, `end`: The backup key range. It's closed left and open right `[start, end)`.
- `format`: Format of `start` and `end`. Supported formats are `raw`、[`hex`](https://en.wikipedia.org/wiki/Hexadecimal) and [`escaped`](https://en.wikipedia.org/wiki/Escape_character).

A progress bar is displayed in the terminal during the backup. When the progress bar advances to 100%, the backup is complete. The progress bar is displayed as follows:
```
br backup raw \
    --pd "${PDIP}:2379" \
    --storage "s3://backup-data/2022-09-16/" \
    --log-file backupfull.log
Backup Raw <---------/................................................> 17.12%.
```

After backup finish, the result message is displayed as follows:
```
[2022/09/20 18:01:10.125 +08:00] [INFO] [collector.go:67] ["Raw backup success summary"] [total-ranges=3] [ranges-succeed=3] [ranges-failed=0] [backup-total-regions=3] [total-take=5.050265883s] [backup-ts=436120585518448641] [total-kv=100000] [total-kv-size=108.7MB] [average-speed=21.11MB/s] [backup-data-size(after-compressed)=78.3MB]
```
Explanations for the above message are as follows: 
- `total-ranges`: Number of ranges that the whole backup task is split to. Equals to `ranges-succeed` + `ranges-failed`.
- `ranges-succeed`: Number of succeeded ranges.
- `ranges-failed`: Number of failed ranges.
- `backup-total-regions`: The tikv regions that backup takes.
- `total-take`: The backup duration.
- `backup-ts`: The backup start timestamp, only take effect for APIV2 TiKV cluster, which can be used as `start-ts` of `TiKV-CDC` when creating replication tasks. Refer to [Create a replication task](https://github.com/tikv/migration/blob/main/cdc/README.md#create-a-replication-task).
- `total-kv`: Total kv count in backup files.
- `total-kv-size`: Total kv size in backup files. Note that this is the original size before compression.
- `average-speed`: The backup speed, which approximately equals to `total-kv-size` / `total-take`.
- `backup-data-size(after-compressed)`: The backup file size.

#### Restore Raw Data

To restore raw data to the cluster, execute the `tikv-br restore raw` command. To get help on this command, execute `tikv-br restore raw -h` or `tikv-br restore raw --help`.
For example, restore the raw backup files in s3 `/backup-data/2022-09-16` to `TiKV` cluster.

```
export AWS_ACCESS_KEY_ID=&{AWS_KEY_ID};
export AWS_SECRET_ACCESS_KEY=&{AWS_KEY};
tikv-br restore raw \
    --pd "${PDIP}:2379" \
    --storage "s3://backup-data/2022-09-16/" \
    --ratelimit 128 \
    --log-file restoreraw.log
```
Explanations for some options in the above command are as follows:

- `--ratelimit`: The maximum speed at which a restoration operation is performed (MiB/s) on each `TiKV` node.
- `--log-file`: Writing the TiKV-BR log to the `restorefull.log` file.

A progress bar is displayed in the terminal during the restoration. When the progress bar advances to 100%, the restoration is complete. The progress bar is displayed as follows:
```
tikv-br restore raw \
    --pd "${PDIP}:2379" \
    --storage "s3://backup-data/2022-09-16/" \
    --ratelimit 128 \
    --log-file restoreraw.log
Restore Raw <---------/...............................................> 17.12%.
```

After restoration finish, the result message is displayed as follows:
```
[2022/09/20 18:02:12.540 +08:00] [INFO] [collector.go:67] ["Raw restore success summary"] [total-ranges=3] [ranges-succeed=3] [ranges-failed=0] [restore-files=3] [total-take=950.460846ms] [restore-data-size(after-compressed)=78.3MB] [total-kv=100000] [total-kv-size=108.7MB] [average-speed=114.4MB/s]
```
Explanations for the above message are as follows: 
- `total-ranges`: Number of ranges that the whole backup task is split to. Equals to `ranges-succeed` + `ranges-failed`.
- `ranges-succeed`: Number of succeeded ranges.
- `ranges-failed`: Number of failed ranges.
- `restore-files`: Number of restored files.
- `total-take`: The restoration duration.
- `total-kv`: Total restored kv count.
- `total-kv-size`: Total restored kv size. Note that this is the original size before compression.
- `average-speed`: The restoration speed, which approximately equals to `total-kv-size` / `total-take`.
- `restore-data-size(after-compressed)`: The restoration file size.


### Data Verification of Backup & Restore

TiKV-BR can do checksum between TiKV cluster and backup files after backup or restoration finish with the config `--checksum=true`. Checksum is using the [checksum](https://github.com/tikv/client-go/blob/ffaaf7131a8df6ab4e858bf27e39cd7445cf7929/rawkv/rawkv.go#L584) interface in TiKV [client-go](https://github.com/tikv/client-go), which send checksum request to all TiKV regions to calculate the checksum of all **VALID** data. Then compare to the checksum value of backup files which is calculated during backup process.

In some scenario, data is stored in TiKV with [TTL](https://docs.pingcap.com/tidb/stable/tikv-configuration-file#enable-ttl). If data is expired during backup & restore, the persisted checksum in backup files is different from the checksum of TiKV cluster. So checksum should not enabled in this scenario. User can perform a full comparison for all existing non-expired data between backup cluster and restore cluster with [scan](https://github.com/tikv/client-go/blob/ffaaf7131a8df6ab4e858bf27e39cd7445cf7929/rawkv/rawkv.go#L492) interface in [client-go](https://github.com/tikv/client-go).

### Security During Backup & Restoration

TiKV-BR support TLS if [TLS config](https://docs.pingcap.com/tidb/dev/enable-tls-between-components) in TiKV cluster is enabled.

Please specify the client certification with config `--ca`, `--cert` and `--key`.

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

TiKV-BR is under the Apache 2.0 license. See the [LICENSE](./LICENSE.md) file for details.
