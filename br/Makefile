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

PROJECT=br
GOPATH ?= $(shell go env GOPATH)
P=8

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif
FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1 } }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH))):$(PWD)/tools/bin
export PATH := $(path_to_add):$(PATH)

GO              := GO111MODULE=on go
GOBUILD         := $(GO) build $(BUILD_FLAG) -tags codes
GOTEST          := $(GO) test -p $(P)
OVERALLS        := GO111MODULE=on overalls
STATICCHECK     := GO111MODULE=on staticcheck

LINUX     := "Linux"
MAC       := "Darwin"

FAILPOINT_ENABLE  := find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl enable
FAILPOINT_DISABLE := find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl disable

TARGET = ""

RACE_FLAG =
ifeq ("$(WITH_RACE)", "1")
	RACE_FLAG = -race
	GOBUILD   = GOPATH=$(GOPATH) $(GO) build
endif

CHECK_FLAG =
ifeq ("$(WITH_CHECK)", "1")
	CHECK_FLAG = $(TEST_LDFLAGS)
endif

BR_PKG := github.com/tikv/migration/br
BR_PACKAGES       := go list ./...| grep "github.com\/tikv\/migration\/br"
BR_PACKAGE_DIRECTORIES := $(BR_PACKAGES) | sed 's|github.com/tikv/migration/br/||'
BR_BIN            := bin/br
TEST_DIR          := /tmp/backup_restore_test

TEST_COVERAGE_DIR := "."

.PHONY: build_br clean unit_test check check-static

default: build_br

failpoint-enable: tools/bin/failpoint-ctl
# Converting gofail failpoints...
	@$(FAILPOINT_ENABLE)

failpoint-disable: tools/bin/failpoint-ctl
# Restoring gofail failpoints...
	@$(FAILPOINT_DISABLE)

tools/bin/failpoint-ctl: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/failpoint-ctl github.com/pingcap/failpoint/failpoint-ctl

build_br:
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(BR_BIN) cmd/br/*.go

test: unit_test

unit_test: export ARGS=$$($(BR_PACKAGES))
unit_test:
	@make failpoint-enable
	@export TZ='Asia/Shanghai';
	$(GOTEST) $(RACE_FLAG) -ldflags '$(LDFLAGS)' -tags leak $(ARGS) -coverprofile=coverage.txt || ( make failpoint-disable && exit 1 )
	@make failpoint-disable
unit_test_in_verify_ci: export ARGS=$$($(BR_PACKAGES))
unit_test_in_verify_ci: tools/bin/gotestsum tools/bin/gocov tools/bin/gocov-xml
	@make failpoint-enable
	@export TZ='Asia/Shanghai';
	@mkdir -p $(TEST_COVERAGE_DIR)
	CGO_ENABLED=1 tools/bin/gotestsum --junitfile "$(TEST_COVERAGE_DIR)/br-junit-report.xml" -- $(RACE_FLAG) -ldflags '$(LDFLAGS)' \
	-tags leak $(ARGS) -coverprofile="$(TEST_COVERAGE_DIR)/br_cov.unit_test.plain" || ( make failpoint-disable && exit 1 )
	tools/bin/gocov convert "$(TEST_COVERAGE_DIR)/br_cov.unit_test.plain" | tools/bin/gocov-xml > "$(TEST_COVERAGE_DIR)/br_cov.unit_test.out"
	@make failpoint-disable

check: check-static

check-static: tools/bin/golangci-lint
	GO111MODULE=on CGO_ENABLED=0 tools/bin/golangci-lint run -v $$($(BR_PACKAGE_DIRECTORIES)) --config ../.golangci.yml --timeout 5m

tools/bin/golangci-lint:
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b ./tools/bin v1.41.1

tools/bin/gotestsum: tools/check/go.mod
	cd tools/check && $(GO) build -o ../bin/gotestsum gotest.tools/gotestsum

tools/bin/gocov: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov  github.com/axw/gocov/gocov

tools/bin/gocov-xml: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov-xml github.com/AlekSi/gocov-xml

clean:
	go clean -i ./...
	rm -rf *.out
	rm -rf bin
	rm -rf tools/bin

br_integration_test: br_bins build_br build_for_br_integration_test
	@cd br && tests/run.sh

# TODO: adjust bins when br integraion tests reformat.
br_bins:
	@which bin/tikv-server
	@which bin/pd-server
	@which bin/pd-ctl

build_for_br_integration_test:
	@make failpoint-enable
	($(GOTEST) -c -cover -covermode=count \
		-coverpkg=github.com/tikv/migration/br/... \
		-o $(BR_BIN).test \
		github.com/tikv/migration/br/cmd/br && \
	$(GOBUILD) $(RACE_FLAG) -o bin/rawkv tests/br_rawkv/*.go \
	) || (make failpoint-disable && exit 1)
	@make failpoint-disable
