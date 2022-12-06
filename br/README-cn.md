# TiKV-BR

**TiKV Backup & Restore (TiKV-BR)** 是 TiKV 分布式备份恢复的命令行工具，用于对 TiKV 集群进行数据备份和恢复。
本文介绍了 TiKV-BR 的工作原理、推荐部署配置、使用限制以及几种使用方式。

## 工作原理

TiKV-BR 将备份或恢复操作命令下发到各个 TiKV 节点。TiKV 收到命令后执行相应的备份或恢复操作。
在一次备份或恢复中，各个 TiKV 节点都会有一个对应的备份路径，TiKV 备份时产生的备份文件将会保存在该路径下，恢复时也会从该路径读取相应的备份文件。

<img src="docs/images/tikv-br-architecture.png?sanitize=true" alt="architecture" width="600"/>

## 备份文件类型

备份路径下会生成以下两种类型文件：
- SST 文件：存储 TiKV 备份下来的数据信息，SST 文件的数据格式根据 `backup` 时指定的 `dst-api-version` 来决定
- backupmeta 文件：存储本次备份的元信息，包括备份文件数、备份文件的 Key 区间、备份文件大小和备份文件 Hash (sha256) 值

## 编译 TiKV-BR
```
make build // 生成 debug 版本的 tikv-br 可执行文件
make release // 生成 release 版本的 tikv-br 可执行文件
make test // 运行测试用例
```

*注意: TiKV-BR 只支持使用 `>= 1.8` 版本的 `go` 语言来编译*

编译成功后，会在 `bin` 目录生成二进制文件

## 使用手册

详细信息，请参考 [TiKV-BR 用户文档](https://tikv.org/docs/latest/concepts/explore-tikv-features/backup-restore-cn/)。

## License

TiKV-BR 基于 Apache 2.0 许可，详见 [LICENSE](./LICENSE.md)。
