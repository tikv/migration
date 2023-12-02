# Copyright 2019 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: check test build clean 
default: release
all: check test build clean

SHELL := $(shell /usr/bin/which bash)

# golang
GO          := GO111MODULE=on go
PACKAGES    := go list ./...
DIRECTORIES := $(PACKAGES) | sed 's|github.com/tikv/migration/br/||'

# build & test
BR_BIN_PATH     ?= bin/tikv-br
TEST_BIN_PATH   ?= bin/tikv-br.test
COVERAGE_DIR    ?= build
TEST_PARALLEL   ?= 8
PD_ADDR         ?= 127.0.0.1:2379
BR_LOCAL_STORE  ?= /tmp/backup_restore_test
API_VERSION     ?= 1
CLUSTER_VERSION ?= nightly
TLS_CA          ?= 
TLS_CERT        ?= 
TLS_KEY         ?= 

LDFLAGS += -X "github.com/tikv/migration/br/pkg/version/build.ReleaseVersion=$(shell git describe --tags --dirty --always)"
LDFLAGS += -X "github.com/tikv/migration/br/pkg/version/build.BuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"
LDFLAGS += -X "github.com/tikv/migration/br/pkg/version/build.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/tikv/migration/br/pkg/version/build.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

check: check/tidy check/golangci-lint check/gosec

check/tidy:
	$(eval GO_SUM_TMPFILE := $(shell mktemp -t go.sum.origin.XXXXXXXXXX))
	cp go.sum $(GO_SUM_TMPFILE)
	$(GO) mod tidy
	diff -q go.sum $(GO_SUM_TMPFILE)

check/golangci-lint: tools/bin/golangci-lint
	GO111MODULE=on CGO_ENABLED=0 tools/bin/golangci-lint run -v $$($(DIRECTORIES)) --config ../.golangci.yml --timeout 5m

check/gosec:
	$(GO) install github.com/securego/gosec/v2/cmd/gosec@v2.15.0
	gosec -fmt=junit-xml -out=results.xml -stdout -verbose=text -exclude=G103,G104,G204,G304,G307,G401,G404,G501,G505,G601 ./...

test:
	make failpoint/enable
	export TZ='Asia/Shanghai'
	mkdir -p $(COVERAGE_DIR)
	$(GO) test -p $(TEST_PARALLEL) -race -ldflags '$(LDFLAGS)' -tags leak $$($(PACKAGES)) -coverprofile=$(COVERAGE_DIR)/cov.unit.out || ( make failpoint/disable && exit 1 )
	make failpoint/disable

report_coverage:
	ls $(COVERAGE_DIR)/cov.*
	@bash <(curl -s https://codecov.io/bash) -F br -s $(COVERAGE_DIR) -f '$(COVERAGE_DIR)/cov.*' -t $(TIKV_MIGRATION_CODECOV_TOKEN)

test/integration: build/br-test build/rawkv-integration-test
	mkdir -p $(COVERAGE_DIR)
	./bin/rawkv_test --pd=${PD_ADDR} \
		--br='${TEST_BIN_PATH}' \
		--br-storage=${BR_LOCAL_STORE} \
		--api-version=${API_VERSION} \
		--cluster-version=${CLUSTER_VERSION} \
		--coverage=$(COVERAGE_DIR) \
		--ca=$(TLS_CA) \
		--cert=$(TLS_CERT) \
		--key=$(TLS_KEY)

failpoint/enable: tools/bin/failpoint-ctl
	find `pwd` -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl enable

failpoint/disable: tools/bin/failpoint-ctl
	find `pwd` -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl disable

tools/bin/golangci-lint:
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./tools/bin v1.55.2

tools/bin/failpoint-ctl: tools/check/go.mod
	cd tools/check && $(GO) build -o ../bin/failpoint-ctl github.com/pingcap/failpoint/failpoint-ctl

build:
	CGO_ENABLED=1 $(GO) build -tags codes -gcflags "all=-N -l" -ldflags '$(LDFLAGS)' -o $(BR_BIN_PATH) cmd/br/*.go

release:
	CGO_ENABLED=1 $(GO) build -tags codes -ldflags '$(LDFLAGS)' -o $(BR_BIN_PATH) cmd/br/*.go

build/rawkv-integration-test:
	cd tests/rawkv && $(GO) build -mod=mod -o ../../bin/rawkv_test *.go

build/br-test:
	@make failpoint/enable
	($(GO) test -c -cover -covermode=count \
		-coverpkg=github.com/tikv/migration/br/... \
		-o $(TEST_BIN_PATH) \
		github.com/tikv/migration/br/cmd/br \
	) || (make failpoint/disable && exit 1)
	@make failpoint/disable

clean:
	go clean -i ./...
	rm -rf *.out bin tools/bin
	rm -rf results.xml 
	rm -rf br-junit-report.xml $(COVERAGE_DIR)
	rm -rf $(BR_LOCAL_STORE)
