module github.com/tikv/migration/br

go 1.16

require (
	cloud.google.com/go/storage v1.16.1
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v0.12.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v0.2.0
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/aws/aws-sdk-go v1.35.3
	github.com/cheggaaa/pb/v3 v3.0.8
	github.com/cheynewallace/tabby v1.1.1
	github.com/coreos/go-semver v0.3.0
	github.com/docker/go-units v0.4.0
	github.com/fsouza/fake-gcs-server v1.19.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.2
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20211207042851-78a55fb8e69c
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/tidb v1.1.0-beta.0.20211229105350-1e7f0dcc63b9
	github.com/pingcap/tidb-tools v5.2.2-0.20211019062242-37a8bef2fa17+incompatible
	github.com/pingcap/tidb/parser v0.0.0-20211229105350-1e7f0dcc63b9
	github.com/pingcap/tipb v0.0.0-20220107024056-3b91949a18a7
	github.com/prometheus/client_golang v1.5.1
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/tikv/client-go/v2 v2.0.0-rc.0.20211229051614-62d6b4a2e8f7
	github.com/tikv/pd v1.1.0-beta.0.20211118054146-02848d2660ee
	go.etcd.io/etcd v0.5.0-alpha.5.0.20210512015243-d19fbe541bf9
	go.uber.org/goleak v1.1.12
	go.uber.org/multierr v1.7.0
	go.uber.org/zap v1.19.1
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f
	golang.org/x/oauth2 v0.0.0-20210805134026-6f1e6394065a
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/api v0.54.0
	google.golang.org/grpc v1.40.0
	sourcegraph.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0
)

// cloud.google.com/go/storage will upgrade grpc to v1.40.0
// we need keep the replacement until go.etcd.io supports the higher version of grpc.
replace google.golang.org/grpc => google.golang.org/grpc v1.29.1

replace github.com/Sirupsen/logrus v1.5.0 => github.com/sirupsen/logrus v1.5.0

replace github.com/sirupsen/logrus v1.5.0 => github.com/Sirupsen/logrus v1.5.0

// fix potential security issue(CVE-2020-26160) introduced by indirect dependency.
replace github.com/dgrijalva/jwt-go => github.com/form3tech-oss/jwt-go v3.2.6-0.20210809144907-32ab6a8243d7+incompatible

replace github.com/pingcap/kvproto => github.com/zz-jason/kvproto v0.0.0-20220330093258-c42dd72a7cc6
