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

# test 
TEST_COVERAGE_DIR := .
TEST_PARALLEL = 8

LDFLAGS += -X "github.com/tikv/migration/br/pkg/version/build.ReleaseVersion=$(shell git describe --tags --dirty --always)"
LDFLAGS += -X "github.com/tikv/migration/br/pkg/version/build.BuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"
LDFLAGS += -X "github.com/tikv/migration/br/pkg/version/build.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/tikv/migration/br/pkg/version/build.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

check: 
	@make check/tidy
	@make check/golangci-lint
	@make check/gosec

check/tidy:
	cp go.sum /tmp/go.sum.origin
	$(GO) mod tidy
	diff -q go.sum /tmp/go.sum.origin

check/golangci-lint: 
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./tools/bin v1.41.1
	GO111MODULE=on CGO_ENABLED=0 tools/bin/golangci-lint run -v $$($(DIRECTORIES)) --config ../.golangci.yml --timeout 5m

check/gosec:
	$(GO) install github.com/securego/gosec/v2/cmd/gosec@v2.9.1
	gosec -fmt=junit-xml -out=results.xml -stdout -verbose=text -exclude=G103,G104,G204,G304,G307,G401,G404,G501,G505,G601 ./...

test: build/gocov build/gocov-xml
	@make failpoint/enable
	@export TZ='Asia/Shanghai';
	$(GO) test -p $(TEST_PARALLEL) -race -ldflags '$(LDFLAGS)' -tags leak $$($(PACKAGES)) -coverprofile="$(TEST_COVERAGE_DIR)/br_cov.unit_test.plain" || ( make failpoint/disable && exit 1 )
	tools/bin/gocov convert "$(TEST_COVERAGE_DIR)/br_cov.unit_test.plain" | tools/bin/gocov-xml > "$(TEST_COVERAGE_DIR)/br_cov.unit_test.out"
	@make failpoint/disable

failpoint/enable: build/failpoint-ctl
	find `pwd`/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl enable

failpoint/disable: build/failpoint-ctl
	find `pwd`/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl disable

build:
	CGO_ENABLED=1 $(GO) build -tags codes -ldflags '$(LDFLAGS)' -o bin/br cmd/br/*.go

build/gocov: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov github.com/axw/gocov/gocov

build/gocov-xml: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov-xml github.com/AlekSi/gocov-xml

build/failpoint-ctl: tools/check/go.mod
	cd tools/check && $(GO) build -o ../bin/failpoint-ctl github.com/pingcap/failpoint/failpoint-ctl

clean:
	go clean -i ./...
	rm -rf *.out
	rm -rf bin
	rm -rf tools/bin
