# TiKV-CDC 使用手册

## 部署

### 使用 TiUP 部署包含 TiKV-CDC 组件的全新 TiKV 集群
_（注意：支持 TiKV-CDC 的 TiUP 版本未正式发布）_

在使用 [TiUP](https://docs.pingcap.com/zh/tidb/stable/production-deployment-using-tiup) 部署全新 TiKV 集群时，支持同时部署 TiKV-CDC 组件。只需在 TiUP 启动 TiKV 集群时的配置文件中加入 TiKV-CDC 部分即可，配置文件参考[模板](https://github.com/tikv/migration/blob/main/cdc/deployments/tikv-cdc/config-templates/topology.example.yaml)。

### 使用 TiUP 在原有 TiKV 集群上新增 TiKV-CDC 组件
_（注意：支持 TiKV-CDC 的 TiUP 版本未正式发布）_

目前也支持在原有 TiKV 集群上使用 TiUP 新增 TiKV-CDC 组件，操作步骤如下：

1. 确认当前 TiKV 集群的版本 >= `v6.2.0`。
2. 根据[模板](https://github.com/tikv/migration/blob/main/cdc/deployments/tikv-cdc/config-templates/scale-out.example.yaml)创建扩容配置文件。
3. 通过 `tiup cluster scale-out` 扩容 TiKV-CDC 组件（TiUP 扩容可参考 [使用 TiUP 扩容缩容 TiDB 集群](https://docs.pingcap.com/zh/tidb/stable/scale-tidb-using-tiup)）。
```
tiup cluster scale-out <cluster-name> scale-out.yaml
```

### 手工部署

1. 部署两个 TiKV 集群，分别作为上游集群和下游集群。
2. 启动 TiKV-CDC 集群，包含一个或多个 TiKV-CDC servers。TiKV-CDC server 的启动命令是 `tikv-cdc server --pd <upstream PD endpoints>`.
3. 通过以下命令启动同步任务：`tikv-cdc cli changefeed create --pd <upstream PD endpoints> --sink-uri tikv://<downstream PD endpoints>`

### TiKV-CDC server 启动参数
* addr：TiKV-CDC 的监听地址，提供服务的 HTTP API 查询地址和 Prometheus 查询地址，默认为 127.0.0.1:8600。
* advertise-addr：TiKV-CDC 对外开放地址，供客户端访问。如果未设置该参数值，地址默认与 addr 相同。
* pd：TiKV-CDC 监听的 PD 节点地址，用英文逗号（`,`）来分隔多个 PD 节点地址。
* config：可选项，表示 TiKV-CDC 使用的配置文件地址。
* data-dir：指定 TiKV-CDC 使用硬盘储存文件时的目录，主要用于外部排序。建议确保该目录所在设备的可用空间大于等于 500 GiB。
* gc-ttl：TiKV-CDC 在 PD 设置的服务级别 GC safepoint 的 TTL (Time To Live) 时长，和 TiKV-CDC 同步任务所能够停滞的时长。单位为秒，默认值为 86400，即 24 小时。注意：TiKV-CDC 同步任务的停滞会影响集群 GC safepoint 的推进。`gc-ttl` 越长，同步任务可以停滞的时间也越久，但同时需要保存更多的过期数据、占用更多的存储空间。反之亦然。
* log-file：TiKV-CDC 进程运行时日志的输出地址，未设置时默认为标准输出 (stdout)。
* log-level：TiKV-CDC 进程运行时的日志级别，默认为 "info"。

## 运维管理

### 管理 TiKV-CDC 服务进程 (`capture`)

#### 查询 `capture` 列表
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

在以上结果中：

* `id`: 服务进程的 ID。
* `is-owner`: 表示该服务进程是否为 owner 节点。
* `address`: 该服务进程对外提供接口的地址。


### 管理同步任务 (`changefeed`)

#### 创建同步任务
```
tikv-cdc cli changefeed create --pd=http://192.168.100.122:2379 --sink-uri="tikv://192.168.100.61:2379/" --changefeed-id="rawkv-replication-task"
```
```
Create changefeed successfully!
ID: rawkv-replication-task
Info: {"sink-uri":"tikv://192.168.100.61:2379","opts":{},"create-time":"2022-07-20T15:35:47.860947953+08:00","start-ts":434714063103852547,"target-ts":0,"admin-job-type":0,"sort-engine":"unified","sort-dir":"","scheduler":{"type":"keyspan-number","polling-time":-1},"state":"normal","history":null,"error":null}
```

在以上命令和结果中：

* `--changefeed-id`: 同步任务的 ID，格式需要符合正则表达式 ^[a-zA-Z0-9]+(\-[a-zA-Z0-9]+)*$。如果不指定该 ID，TiKV-CDC 会自动生成一个 UUID（version 4 格式）作为 ID。

* `--sink-uri`: 同步任务下游的地址，需要按照以下格式进行配置。目前 scheme 仅支持 `tikv`。此外，如果 URI 中包含特殊字符时，需要以 URL 编码对特殊字符进行处理。

```
[scheme]://[userinfo@][host]:[port][/path]?[query_parameters]
```

* `--start-ts`: 指定 changefeed 的开始 TSO。TiKV-CDC 集群将从这个 TSO 开始拉取数据。默认为当前时间。如果在一个存量集群上部署同步任务，建议使用 [TiKV-BR](https://github.com/tikv/migration/tree/main/br) 对现有数据进行备份并恢复到下游集群，并从 `TiKV-BR` 的备份结果中获取 `backup-ts` 作为 `start-ts` （`--start-ts=<backup-ts+1>`）.

#### Sink URI 配置 `tikv`
```
--sink-uri="tikv://192.168.100.61:2379/"
```
| 参数 | 说明                                                                  |
|---------------------------|------------------------------------------------------------------------------|
| 192.168.100.61:2379          | 下游 PD 地址。多个地址用英文逗号（`,`）分隔。 |

#### 查询同步任务列表
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

在以上结果中：

* `checkpoint`  即为 TiKV-CDC 已经将该时间点前的数据同步到了下游。
* `state` 为该同步任务的状态：
  * `normal`: 正常同步
  * `stopped`: 停止同步（手动暂停）
  * `error`: 停止同步（出错）
  * `removed`: 已删除任务（只在指定 --all 选项时才会显示该状态的任务）


#### 查询特定同步任务
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

在以上结果中：

* `state` 代表当前 changefeed 的同步状态，与 changefeed list 中的状态相同。
* `tso` 代表当前 changefeed 中已经成功写入下游的最大事务 TSO。
* `checkpoint` 代表当前 changefeed 中已经成功写入下游的最大事务 TSO 对应的时间。
* `error` 记录当前 changefeed 是否有错误发生。


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

在以上结果中：

* `info`  代表查询 changefeed 的同步配置。
* `status` 代表查询 changefeed 的同步状态信息。
* `resolved-ts`: 代表当前 changefeed 中已经成功从 TiKV 发送到 TiKV-CDC 的最大 TS。
* `checkpoint-ts`: 代表当前 changefeed 中已经成功写入下游的最大 TS。
* `admin-job-type`: 代表一个 changefeed 的状态：
  * `0`: 状态正常。
  * `1`: 任务暂停，停止任务后所有同步 processor 会结束退出，同步任务的配置和同步状态都会保留，可以从 checkpoint-ts 恢复任务。
  * `2`: 任务恢复，同步任务从 checkpoint-ts 继续同步。
  * `3`: 任务已删除，接口请求后会结束所有同步 processor，并清理同步任务配置信息。同步状态保留，只提供查询，没有其他实际功能。
* `task-status` 代表查询 changefeed 所分配的各个同步子任务的状态信息。


#### 停止同步任务
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

在以上命令中：

* `--changefeed-id=uuid` 为需要操作的 `changefeed` ID。


#### 恢复同步任务
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

#### 删除同步任务
```
tikv-cdc cli changefeed remove --changefeed-id rawkv-replication-task --pd=http://192.168.100.122:2379
tikv-cdc cli changefeed list --pd=http://192.168.100.122:2379
```
```
[]
```

### 查询同步子任务处理单元 (processor)
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
