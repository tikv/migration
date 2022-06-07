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
default: build
all: check test build clean

# golang
GO          := GO111MODULE=on go
PACKAGES    := go list ./...
DIRECTORIES := $(PACKAGES) | sed 's|github.com/tikv/migration/br/||'

# build & test
BR_BIN_PATH    ?= bin/tikv-br
TEST_BIN_PATH  ?= bin/tikv-br.test
COVERAGE_DIR   ?= build
TEST_PARALLEL  ?= 8
PD_ADDR        ?= 127.0.0.1:2379
BR_LOCAL_STORE ?= /tmp/backup_restore_test

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
	$(GO) install github.com/securego/gosec/v2/cmd/gosec@v2.9.1
	gosec -fmt=junit-xml -out=results.xml -stdout -verbose=text -exclude=G103,G104,G204,G304,G307,G401,G404,G501,G505,G601 ./...

test: tools/bin/gocov tools/bin/gocov-xml
	make failpoint/enable
	export TZ='Asia/Shanghai'
	mkdir -p $(COVERAGE_DIR)
	$(GO) test -p $(TEST_PARALLEL) -race -ldflags '$(LDFLAGS)' -tags leak $$($(PACKAGES)) -coverprofile=$(COVERAGE_DIR)/coverage.raw || ( make failpoint/disable && exit 1 )
	tools/bin/gocov convert $(COVERAGE_DIR)/coverage.raw | tools/bin/gocov-xml > $(COVERAGE_DIR)/coverage.xml
	make failpoint/disable

test/integration: build/br-test build/rawkv-helper
	./tests/rawkv/run.py --test-helper=bin/rawkv --pd=$(PD_ADDR) \
		--br='$(TEST_BIN_PATH) -test.coverprofile=cov.br_rawkv.out.log DEVEL' \
		--br-storage=local://$(BR_LOCAL_STORE)

failpoint/enable: tools/bin/failpoint-ctl
	find `pwd` -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl enable

failpoint/disable: tools/bin/failpoint-ctl
	find `pwd` -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl disable

tools/bin/golangci-lint:
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./tools/bin v1.41.1

tools/bin/gocov: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov github.com/axw/gocov/gocov

tools/bin/gocov-xml: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov-xml github.com/AlekSi/gocov-xml

tools/bin/failpoint-ctl: tools/check/go.mod
	cd tools/check && $(GO) build -o ../bin/failpoint-ctl github.com/pingcap/failpoint/failpoint-ctl

build:
	CGO_ENABLED=1 $(GO) build -tags codes -gcflags "all=-N -l" -ldflags '$(LDFLAGS)' -o $(BR_BIN_PATH) cmd/br/*.go

release:
	CGO_ENABLED=1 $(GO) build -tags codes -ldflags '$(LDFLAGS)' -o $(BR_BIN_PATH) cmd/br/*.go

build/rawkv-helper:
	cd tests/rawkv && $(GO) build -mod=mod -o ../../bin/rawkv client.go

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
