# TiKV-CDC

[![Build Status](https://github.com/tikv/migration/actions/workflows/ci-cdc.yml/badge.svg)](https://github.com/tikv/migration/actions/workflows/ci-cdc.yml)
[![codecov](https://codecov.io/gh/tikv/migration/branch/main/graph/badge.svg?token=7nmbrqKeWs)](https://codecov.io/gh/tikv/migration)
[![LICENSE](https://img.shields.io/github/license/tikv/migration)](https://github.com/tikv/migration/blob/main/cdc/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/tikv/migration/cdc)](https://goreportcard.com/report/github.com/tikv/migration/cdc)

**TiKV-CDC** is [TiKV](https://docs.pingcap.com/tidb/dev/tikv-overview)'s change data capture framework. It supports replicating change data to another TiKV cluster.

TiKV-CDC is a fork of [TiCDC](https://github.com/pingcap/tiflow/blob/master/README_TiCDC.md), but focus on NoSQL scenario that uses TiKV as a Key-Value storage. By now, it can be used by RawKV to build a storage system with Cross Cluster Replication, to provide financial-level disaster recovery capabilities.

Please note that the minimal required version of TiKV is `v6.2.0`.

## Architecture

TODO

## Building

To check the source code, run test cases and build binaries, you can simply run:

```
$ make dev
$ make cdc
```

## Deployment

### Deploy by TiUP

_Note: TiUP >= `v1.11.0` is required. This version will be released before the end of Sep, 2022_

#### Deploy a new TiDB/TiKV cluster including TiKV-CDC

When you deploy a new TiDB/TiKV cluster using TiUP, you can also deploy TiKV-CDC at the same time. You only need to add the `kvcdc_servers` section in the initialization configuration file that TiUP uses to start the TiDB/TiKV cluster. Please refer to the configuration [template](https://github.com/tikv/migration/blob/main/cdc/deployments/tikv-cdc/config-templates/topology.example.yaml).

#### Add TiKV-CDC to an existing TiDB/TiKV cluster

You can also use TiUP to add the TiKV-CDC component to an existing TiDB/TiKV cluster. Take the following procedures:

1. Make sure that the current TiDB/TiKV version >= `6.2.0`.
2. Prepare a scale-out configuration file, refer to [template](https://github.com/tikv/migration/blob/main/cdc/deployments/tikv-cdc/config-templates/scale-out.example.yaml).
3. Scale out by `tiup cluster scale-out`. Also Refer to [Scale a TiDB Cluster Using TiUP](https://docs.pingcap.com/tidb/stable/scale-tidb-using-tiup).
```
tiup cluster scale-out <cluster-name> scale-out.yaml
```

### Deploy manually

1. Set up two TiKV cluster, one for upstream and another for downstream.
2. Start a TiKV-CDC cluster, which contains one or more TiKV-CDC servers. The command to start on TiKV-CDC server is `tikv-cdc server --pd <upstream PD endpoints>`.
3. Start a replication changefeed by `tikv-cdc cli changefeed create --pd <upstream PD endpoints> --sink-uri tikv://<downstream PD endpoints>`

### Arguments for start TiKV-CDC server
* `addr`: The listening address of TiKV-CDC, the HTTP API address, and the Prometheus address of the TiKV-CDC service. The default value is 127.0.0.1:8600.
* `advertise-addr`: The advertised address via which clients access TiKV-CDC. If unspecified, the value is the same as `addr`.
* `pd`: A comma-separated list of PD endpoints.
* `config`: The address of the configuration file that TiKV-CDC uses (optional).
* `data-dir`: Specifies the directory that TiKV-CDC uses to store temporary files for sorting. It is recommended to ensure that the free disk space for this directory is greater than or equal to 500 GiB.
* `gc-ttl`: The TTL (Time To Live) of the service level `GC safepoint` in PD set by TiKV-CDC, and the duration that the replication task can suspend, in seconds. The default value is 86400, which means 24 hours. Note: Suspending of the TiKV-CDC replication task affects the progress of TiKV-CDC `GC safepoint`. The longer of `gc-ttl`, the longer a changefeed can be paused, but at the same time more outdate data will be kept, larger space will be occupied. And vice versa.
* `log-file`: The path to which logs are output when the TiKV-CDC process is running. If this parameter is not specified, logs are written to the standard output (stdout).
* `log-level`: The log level when the TiKV-CDC process is running. The default value is "info".

## Maintain

### Manage TiKV-CDC service (`capture`)

#### Query the `capture` list
```
tikv-cdc cli capture list --pd=http://192.168.100.122:2379
```
```
[
  {
    "id": "07684765-52df-42a0-8dd1-a4e9084bb7c1",
    "is-owner": false,
    "address": "192.168.100.9:8600"
  },
  {
    "id": "aea1445b-c065-4dc5-be53-a445261f7fc2",
    "is-owner": true,
    "address": "192.168.100.26:8600"
  },
  {
    "id": "f29496df-f6b4-4c1e-bfa3-41a058ce2144",
    "is-owner": false,
    "address": "192.168.100.142:8600"
  }
]
```

In the result above:

* `id`: The ID of the service process.
* `is-owner`: Indicates whether the service process is the owner node.
* `address`: The address via which the service process provides interface to the outside.


### Manage Replication Tasks (`changefeed`)

#### Create a replication task
```
tikv-cdc cli changefeed create --pd=http://192.168.100.122:2379 --sink-uri="tikv://192.168.100.61:2379/" --changefeed-id="rawkv-replication-task"
```
```
Create changefeed successfully!
ID: rawkv-replication-task
Info: {"sink-uri":"tikv://192.168.100.61:2379","opts":{},"create-time":"2022-07-20T15:35:47.860947953+08:00","start-ts":434714063103852547,"target-ts":0,"admin-job-type":0,"sort-engine":"unified","sort-dir":"","scheduler":{"type":"keyspan-number","polling-time":-1},"state":"normal","history":null,"error":null}
```

In the command and result above:

* `--changefeed-id`: The ID of the replication task. The format must match the ^[a-zA-Z0-9]+(\-[a-zA-Z0-9]+)*$ regular expression. If this ID is not specified, TiKV-CDC automatically generates a UUID (the version 4 format) as the ID.

* `--sink-uri`: The downstream address of the replication task. Configure --sink-uri according to the following format. Currently, the scheme supports `tikv` only. Besides, when a URI contains special characters, you need to process these special characters using URL encoding.

```
[scheme]://[userinfo@][host]:[port][/path]?[query_parameters]
```

* `--start-ts`: Specifies the starting TSO of the changefeed. From this TSO, the TiKV-CDC cluster starts pulling data. The default value is the current time. If the replication is deployed on a existing cluster, it is recommended that using [TiKV-BR](https://github.com/tikv/migration/tree/main/br) to backup & restore existing data to downstream, get `backup-ts` from backup result of `TiKV-BR`, and then deploy changefeed with `--start-ts=<backup-ts+1>`.

#### Configure sink URI with `tikv`
```
--sink-uri="tikv://192.168.100.61:2379/"
```
| Parameter/Parameter Value | Description                                                                  |
|---------------------------|------------------------------------------------------------------------------|
| 192.168.100.61:2379          | The endpoints of the downstream PD. Multiple addresses are separated by comma. |

#### Query the replication task list
```
tikv-cdc cli changefeed list --pd=http://192.168.100.122:2379
```
```
[
  {
    "id": "rawkv-replication-task",
    "summary": {
      "state": "normal",
      "tso": 434715745556889877,
      "checkpoint": "2022-07-20 17:22:45.900",
      "error": null
    }
  }
]
```

In the result above:

* `checkpoint` indicates that TiKV-CDC has already replicated data before this time point to the downstream.
* `state` indicates the state of the replication task.
  * `normal`: The replication task runs normally.
  * `stopped`: The replication task is stopped (manually paused).
  * `error`: The replication task is stopped (by an error).
  * `removed`: The replication task is removed. Tasks of this state are displayed only when you have specified the --all option. To see these tasks when this option is not specified, execute the changefeed query command.


#### Query a specific replication task
```
tikv-cdc cli changefeed query -s --changefeed-id rawkv-replication-task --pd=http://192.168.100.122:2379
```
```
{
 "state": "normal",
 "tso": 434716089136185435,
 "checkpoint": "2022-07-20 17:44:36.551",
 "error": null
}
```

In the result above:

* `state` is the replication state of the current changefeed. Each state must be consistent with the state in changefeed list.
* `tso` represents the largest TSO in the current changefeed that has been successfully replicated to the downstream.
* `checkpoint` represents the corresponding time of the largest TSO in the current changefeed that has been successfully replicated to the downstream.
* `error` records whether an error has occurred in the current changefeed.


```
tikv-cdc cli changefeed query --changefeed-id rawkv-replication-task --pd=http://192.168.100.122:2379
```
```
{
  "info": {
    "sink-uri": "tikv://192.168.100.61:2379/",
    "opts": {},
    "create-time": "2022-07-20T17:21:54.115625346+08:00",
    "start-ts": 434715731964985345,
    "target-ts": 0,
    "admin-job-type": 0,
    "sort-engine": "unified",
    "sort-dir": "",
    "config": {
      "check-gc-safe-point": true,
      "scheduler": {
        "type": "keyspan-number",
        "polling-time": -1
      },
    },
    "state": "normal",
    "history": null,
    "error": null,
    "sync-point-enabled": false,
    "sync-point-interval": 600000000000,
  },
  "status": {
    "resolved-ts": 434715754364928912,
    "checkpoint-ts": 434715754103047044,
    "admin-job-type": 0
  },
  "count": 0,
  "task-status": [
    {
      "capture-id": "aea1445b-c065-4dc5-be53-a445261f7fc2",
      "status": {
        "keyspans": {
          "15137828009456710810": {
            "start-ts": 434715731964985345,
            "Start": "cg==",
            "End": "cw=="
          }
        },
        "operation": {},
        "admin-job-type": 0
      }
    }
  ]
}
```

In the result above:

* `info` is the replication configuration of the queried changefeed.
* `status` is the replication state of the queried changefeed.
* `resolved-ts`: The largest TS in the current changefeed. Note that this TS has been successfully sent from TiKV to TiKV-CDC.
* `checkpoint-ts`: The largest TS in the current changefeed. Note that this TS has been successfully written to the downstream.
* `admin-job-type`: The status of a changefeed:
  * `0`: The state is normal.
  * `1`: The task is paused. When the task is paused, all replicated processors exit. The configuration and the replication status of the task are retained, so you can resume the task from `checkpoint-ts`.
  * `2`: The task is resumed. The replication task resumes from `checkpoint-ts`.
  * `3`: The task is removed. When the task is removed, all replicated processors are ended, and the configuration information of the replication task is cleared up. Only the replication status is retained for later queries.
* `task-status` indicates the state of each replication sub-task in the queried changefeed.


#### Pause a replication task
```
tikv-cdc cli changefeed pause --changefeed-id rawkv-replication-task --pd=http://192.168.100.122:2379
tikv-cdc cli changefeed list --pd=http://192.168.100.122:2379
```
```
[
  {
    "id": "rawkv-replication-task",
    "summary": {
      "state": "stopped",
      "tso": 434715759083521004,
      "checkpoint": "2022-07-20 17:23:37.500",
      "error": null
    }
  }
]
```

In the command above:

* `--changefeed-id=uuid` represents the ID of the changefeed that corresponds to the replication task you want to pause.


#### Resume a replication task
```
tikv-cdc cli changefeed resume --changefeed-id rawkv-replication-task --pd=http://192.168.100.122:2379
tikv-cdc cli changefeed list --pd=http://192.168.100.122:2379
```
```
[
  {
    "id": "rawkv-replication-task",
    "summary": {
      "state": "normal",
      "tso": 434715759083521004,
      "checkpoint": "2022-07-20 17:23:37.500",
      "error": null
    }
  }
]
```

#### Remove a replication task
```
tikv-cdc cli changefeed remove --changefeed-id rawkv-replication-task --pd=http://192.168.100.122:2379
tikv-cdc cli changefeed list --pd=http://192.168.100.122:2379
```
```
[]
```

### Query processing units of replication sub-tasks (processor)
```
tikv-cdc cli processor list --pd=http://192.168.100.122:2379`
```
```
[
  {
    "changefeed_id": "rawkv-replication-task",
    "capture_id": "07684765-52df-42a0-8dd1-a4e9084bb7c1"
  }
]
```

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

TiKV-CDC is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
