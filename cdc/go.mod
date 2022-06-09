module github.com/tikv/migration/cdc

go 1.16

require (
	github.com/BurntSushi/toml v1.1.0
	github.com/DataDog/zstd v1.4.6-0.20210211175136-c6db21d202f4 // indirect
	github.com/Shopify/sarama v1.34.1
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751
	github.com/benbjohnson/clock v1.3.0
	github.com/bradleyjkemp/grpc-tools v0.2.7
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/chzyer/readline v1.5.0
	github.com/cockroachdb/pebble v0.0.0-20211124172904-3ca75111760c
	github.com/coreos/go-semver v0.3.0
	github.com/edwingeng/deque v0.0.0-20191220032131-8596380dee17
	github.com/fatih/color v1.13.0
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/gin-gonic/gin v1.7.7
	github.com/go-openapi/spec v0.20.6 // indirect
	github.com/go-openapi/swag v0.21.1 // indirect
	github.com/go-playground/validator/v10 v10.9.0 // indirect
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.1
	github.com/google/go-cmp v0.5.8
	github.com/google/pprof v0.0.0-20220520215854-d04f2422c8a1 // indirect
	github.com/google/uuid v1.1.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/integralist/go-findroot v0.0.0-20160518114804-ac90681525dc
	github.com/jarcoal/httpmock v1.0.8
	github.com/jmoiron/sqlx v1.3.5
	github.com/jonboulle/clockwork v0.3.0 // indirect
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/mattn/go-shellwords v1.0.12
	github.com/modern-go/reflect2 v1.0.2
	github.com/phayes/freeport v0.0.0-20220201140144-74d24b5ae9f5
	github.com/pingcap/check v0.0.0-20211026125417-57bd13f7b5f0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20220423142525-ae43b7f4e5c3
	github.com/pingcap/kvproto v0.0.0-20220525022339-6aaebf466305
	github.com/pingcap/log v1.1.0
	github.com/pingcap/tidb v1.1.0-beta.0.20220528045048-5495dc6c4360
	github.com/pingcap/tidb-tools v6.0.1-0.20220516050036-b3ea358e374a+incompatible
	github.com/pingcap/tidb/parser v0.0.0-20220528045048-5495dc6c4360
	github.com/prometheus/client_golang v1.12.1
	github.com/r3labs/diff v1.1.0
	github.com/shirou/gopsutil/v3 v3.22.5 // indirect
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.4.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/objx v0.4.0 // indirect
	github.com/stretchr/testify v1.7.2
	github.com/swaggo/files v0.0.0-20210815190702-a29dd2bc99b2
	github.com/swaggo/gin-swagger v1.4.3
	github.com/swaggo/swag v1.8.2
	github.com/syndtr/goleveldb v1.0.1-0.20210305035536-64b5b1c73954
	github.com/tikv/client-go/v2 v2.0.1-0.20220531081749-2807409d4968
	github.com/tikv/pd v1.1.0-beta.0.20220530063827-109719ff0875
	github.com/tikv/pd/client v0.0.0-20220530063827-109719ff0875
	github.com/tinylib/msgp v1.1.6
	github.com/tmc/grpc-websocket-proxy v0.0.0-20220101234140-673ab2c3ae75 // indirect
	github.com/twmb/murmur3 v1.1.3
	github.com/uber-go/atomic v1.4.0
	github.com/ugorji/go v1.2.6 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5
	github.com/xdg/scram v1.0.5
	github.com/xitongsys/parquet-go v1.6.0 // indirect
	go.etcd.io/etcd/api/v3 v3.5.4
	go.etcd.io/etcd/client/pkg/v3 v3.5.4
	go.etcd.io/etcd/client/v3 v3.5.4
	go.etcd.io/etcd/server/v3 v3.5.4
	go.uber.org/atomic v1.9.0
	go.uber.org/goleak v1.1.12
	go.uber.org/zap v1.21.0
	golang.org/x/net v0.0.0-20220607020251-c690dde0001d
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f
	golang.org/x/term v0.0.0-20220526004731-065cf7ba2467 // indirect
	golang.org/x/text v0.3.7
	golang.org/x/time v0.0.0-20220411224347-583f2d630306
	golang.org/x/xerrors v0.0.0-20220517211312-f3a8303e98df // indirect
	google.golang.org/grpc v1.47.0
	upper.io/db.v3 v3.8.0+incompatible
)

replace github.com/dgrijalva/jwt-go v3.2.0+incompatible => github.com/golang-jwt/jwt v3.2.1+incompatible

replace github.com/golang-jwt/jwt v3.2.1+incompatible => github.com/golang-jwt/jwt v3.2.0+incompatible

replace github.com/uber-go/atomic => go.uber.org/atomic v1.4.0
