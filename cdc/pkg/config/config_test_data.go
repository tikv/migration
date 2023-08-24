// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

const (
	testCfgTestReplicaConfigOutDated = `{
  "enable-old-value": false,
  "check-gc-safe-point": true,
  "sink": {
    "dispatch-rules": [
      {
        "db-name": "a",
        "tbl-name": "b",
        "rule": "r1"
      },
      {
        "db-name": "a",
        "tbl-name": "c",
        "rule": "r2"
      },
      {
        "db-name": "a",
        "tbl-name": "d",
        "rule": "r2"
      }
    ],
    "protocol": "open-protocol"
  },
  "scheduler": {
    "type": "keyspan-number",
    "polling-time": -1
  }
}`

	testCfgTestServerConfigMarshal = `{
  "addr": "192.155.22.33:8887",
  "advertise-addr": "",
  "log-file": "",
  "log-level": "info",
  "log": {
    "file": {
      "max-size": 300,
      "max-days": 0,
      "max-backups": 0
    },
    "error-output": "stderr"
  },
  "data-dir": "",
  "gc-ttl": 86400,
  "tz": "System",
  "capture-session-ttl": 10,
  "owner-flush-interval": 200000000,
  "processor-flush-interval": 100000000,
  "sorter": {
    "num-concurrent-worker": 4,
    "chunk-size-limit": 999,
    "max-memory-percentage": 30,
    "max-memory-consumption": 17179869184,
    "num-workerpool-goroutine": 16,
    "sort-dir": "/tmp/sorter"
  },
  "security": {
    "ca-path": "",
    "cert-path": "",
    "key-path": "",
    "cert-allowed-cn": null
  },
  "per-changefeed-memory-quota": 1073741824,
  "kv-client": {
    "worker-concurrent": 8,
    "worker-pool-size": 0,
    "region-scan-limit": 40,
    "resolved-ts-safe-interval": 3000000000
  },
  "debug": {
    "enable-keyspan-actor": false,
    "enable-db-sorter": false,
    "db": {
      "count": 8,
      "concurrency": 128,
      "max-open-files": 10000,
      "block-size": 65536,
      "block-cache-size": 4294967296,
      "writer-buffer-size": 8388608,
      "compression": "snappy",
      "target-file-size-base": 8388608,
      "write-l0-slowdown-trigger": 2147483647,
      "write-l0-pause-trigger": 2147483647,
      "compaction-l0-trigger": 160,
      "compaction-deletion-threshold": 160000,
      "iterator-max-alive-duration": 10000,
      "iterator-slow-read-duration": 256,
      "cleanup-speed-limit": 10000
    }
  }
}`

	testCfgTestReplicaConfigMarshal1 = `{
  "enable-old-value": false,
  "check-gc-safe-point": true,
  "sink": {
    "dispatchers": null,
    "protocol": "open-protocol",
    "column-selectors": [
      {
        "matcher": [
          "1.1"
        ],
        "columns": [
          "a",
          "b"
        ]
      }
    ]
  },
  "scheduler": {
    "type": "keyspan-number",
    "polling-time": -1
  },
  "filter": {
    "key-prefix": "prefix",
    "key-pattern": "key\\x00pattern",
    "value-pattern": "value\\ffpattern"
  }
}`

	testCfgTestReplicaConfigMarshal2 = `{
  "enable-old-value": false,
  "check-gc-safe-point": true,
  "sink": {
    "dispatchers": null,
    "protocol": "open-protocol",
    "column-selectors": [
      {
        "matcher": [
          "1.1"
        ],
        "columns": [
          "a",
          "b"
        ]
      }
    ]
  },
  "scheduler": {
    "type": "keyspan-number",
    "polling-time": -1
  },
  "filter": {
    "key-prefix": "prefix",
    "key-pattern": "key\\x00pattern",
    "value-pattern": "value\\ffpattern"
  }
}`
)
