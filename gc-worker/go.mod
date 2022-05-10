// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

module github.com/tikv/migration/gc-worker

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/docker/go-units v0.4.0
	github.com/google/uuid v1.1.2 // indirect
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/kvproto v0.0.0-20220506032820-55094d91343e
	github.com/pingcap/log v0.0.0-20211215031037-e024ba4eb0ee
	github.com/stretchr/testify v1.7.0
	github.com/tikv/pd v1.1.0-beta.0.20211118054146-02848d2660ee
	github.com/tikv/pd/client v0.0.0-20220428091252-fc74bea31d5d
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/atomic v1.9.0
	go.uber.org/zap v1.20.0
	google.golang.org/grpc v1.43.0
)

replace google.golang.org/grpc => google.golang.org/grpc v1.26.0

replace github.com/pingcap/kvproto => github.com/AmoebaProtozoa/kvproto v0.0.0-20220505035154-33f7827ec636

replace github.com/tikv/pd => github.com/AmoebaProtozoa/pd v1.1.0-beta.0.20220510020650-fcc34e174e82

replace github.com/tikv/pd/client => github.com/AmoebaProtozoa/pd/client v0.0.0-20220510020650-fcc34e174e82
