module github.com/tikv/migration/br/tests/br_rawkv

go 1.21

require (
	github.com/coreos/go-semver v0.3.0
	github.com/docker/go-units v0.4.0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/kvproto v0.0.0-20220705053936-aa9c2d20cd2a
	github.com/pingcap/log v0.0.0-20211215031037-e024ba4eb0ee
	github.com/tikv/client-go/v2 v2.0.1-0.20220721031657-e38d2b07de3f
	github.com/tikv/migration/br v0.0.0-20221104062935-658ccf72a949
	github.com/tikv/pd/client v0.0.0-20220307081149-841fa61e9710
	go.uber.org/zap v1.21.0
	golang.org/x/sync v0.1.0
	google.golang.org/grpc v1.56.3
)

require (
	github.com/benbjohnson/clock v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.11.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/stathat/consistent v1.0.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
)

replace github.com/tikv/migration/br => ../../
