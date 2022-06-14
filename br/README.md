# TiKV-BR

**TiKV Backup & Restore (TiKV-BR)** 是 TiKV 分布式备份恢复的命令行工具，用于对 TiKV 集群进行数据备份和恢复。
本文介绍了 TiKV-BR 的工作原理、推荐部署配置、使用限制以及几种使用方式。

## 工作原理

TiKV-BR 将备份或恢复操作命令下发到各个 TiKV 节点。TiKV 收到命令后执行相应的备份或恢复操作。
在一次备份或恢复中，各个 TiKV 节点都会有一个对应的备份路径，TiKV 备份时产生的备份文件将会保存在该路径下，恢复时也会从该路径读取相应的备份文件。

<img src="docs/images/tikv-architecture.png?sanitize=true" alt="architecture" width="600"/>

## 备份文件类型

备份路径下会生成以下两种类型文件：
- SST 文件：存储 TiKV 备份下来的数据信息, SST 文件的数据格式根据 `backup` 时指定的 `dst-api-version` 来决定
- backupmeta 文件：存储本次备份的元信息，包括备份文件数、备份文件的 Key 区间、备份文件大小和备份文件 Hash (sha256) 值

## 编译 TiKV-BR
```
make build // 生成 debug 版本的 tikv-br 可执行文件
make release // 生成 release 版本的 tikv-br 可执行文件
make test // 运行测试用例
```

*注意: TiKV-BR 只支持使用 `>= 1.8` 版本的 `go` 语言来编译*

编译成功后，会在 `bin` 目录生成二进制文件

## 部署使用 TiKV-BR 工具

### 推荐部署配置
- 推荐 TiKV-BR 部署在 PD 节点上。
- 推荐使用一块高性能 SSD 网盘，挂载到 TiKV-BR 节点和所有 TiKV 节点上，网盘推荐万兆网卡，否则带宽有可能成为备份恢复时的性能瓶颈。

### 最佳实践
下面是使用 TiKV-BR 进行备份恢复的几种推荐操作：
- 推荐在业务低峰时执行备份操作，这样能最大程度地减少对业务的影响。
- TiKV-BR 支持在不同拓扑的集群上执行恢复，但恢复期间对在线业务影响很大，建议低峰期或者限速 (rate-limit) 执行恢复。
- TiKV-BR 备份最好串行执行。不同备份任务并行会导致备份性能降低，同时也会影响在线业务。
- TiKV-BR 恢复最好串行执行。不同恢复任务并行会导致 Region 冲突增多，恢复的性能降低。
- 推荐在 -s 指定的备份路径上挂载一个共享存储，例如 NFS。这样能方便收集和管理备份文件。
- 在使用共享存储时，推荐使用高吞吐的存储硬件，因为存储的吞吐会限制备份或恢复的速度。
- TiKV-BR 默认会分别在备份、恢复完成后，可以通过指定 --checksum=true进行一轮数据校验，将文本数据同集群数据比较，来保证正确性。如果 TiKV 集群开启了 TTL，请注意，在备份、恢复过程中如果有数据 TTL 失效，引发数据过期删除，此时 checksum 会不一致，此时并不代表数据存在一致性问题。
- TiKV-BR 支持不同 api-version 的升级备份，即可以通过指定 `--dst-api-version V2` 将 `api-version=1` 的 TiKV 集群备份为 `api-version=2` 的 `SST` 文件，并将备份文件恢复到 `api-version=2` 的 TiKV 集群中。

### TiKV-BR 命令行描述
一条 `tikv-br` 命令是由子命令、选项和参数组成的。子命令即不带 `-` 或者 `--` 的字符。选项即以 `-` 或者 `--` 开头的字符。参数即子命令或选项字符后紧跟的、并传递给命令和选项的字符。
#### 备份集群 Raw 模式数据
要备份 TiKV 集群中 Raw 模式数据，可使用 `tikv-br backup raw` 命令。该命令的使用帮助可以通过 `tikv-br backup raw --help` 来获取。
用例：将 TiKV 集群中 Raw 模式数据备份到 `/tmp/backup` 目录中。
```
tikv-br backup raw --pd "&{PDIP}:2379" -s "local:///tmp/backup" --dst-api-version v2 --log-file="/tmp/br_backup.log
```
命令行各部分的解释如下：
- `backup`：`tikv-br` 的子命令
- `raw`：`backup` 的子命令
- `-s` 或 `--storage`：备份保存的路径
- `"local:///tmp/backup"`：`-s` 的参数，保存的路径为各个 TiKV 节点本地磁盘的 `/tmp/backup`
- `--pd`：`PD` 服务地址
- `"${PDIP}:2379"`：`--pd` 的参数
- `--dst-api-version`: 指定备份文件的 `api-version`，请见 tikv config
- `v2`: `--dst-api-version` 的参数，可选参数为 `v1`, `v1ttl`, `v2`(不区分大小写), 如果不指定 `dst-api-version` 参数，则备份文件的 `api-version` 与指定 `--pd` 所属的 TiKV 集群 `api-version` 相同。

#### 恢复 Raw 模式备份数据

要将 Raw 模式备份数据恢复到集群中来，可使用 `tikv-br restore raw` 命令。该命令的使用帮助可以通过 `tikv-br restore raw --help` 来获取。
用例：将 `/tmp/backup` 路径中的 Raw 模式备份数据恢复到集群中。
```
tikv-br restore raw \
    --pd "${PDIP}:2379" \
    --storage "local:///tmp/backup" \
    --log-file restoreraw.log
```
以上命令中，`--log-file` 选项指定把 `TiKV-BR` 的 log 写到 `restoreraw.log` 文件中。
恢复期间还有进度条会在终端中显示，当进度条前进到 100% 时，说明恢复已完成。可以通过指定 `--checksum=true` 在 `restore` 结束时进行一轮数据校验，将文本数据同集群数据比较，来保证正确性。请注意: 如果 TiKV 集群开启了 `TTL`，在备份恢复过程中如果有数据 TTL 失效，引发数据过期删除，此时 `checksum` 不一致，并不代表数据存在一致性问题。

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

TiKV-BR is under the Apache 2.0 license. See the [LICENSE](./LICENSE.md) file for details.
