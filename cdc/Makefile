### Makefile for ticdc
.PHONY: build uint_test lint

PROJECT=migration/cdc
P=3

FAIL_ON_STDOUT := awk '{ print  } END { if (NR > 0) { exit 1  }  }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(CURDIR)/bin:$(path_to_add):$(PATH)

SHELL := /usr/bin/env bash

TEST_DIR := /tmp/tidb_cdc_test

GO       := GO111MODULE=on go
ifeq (${CDC_ENABLE_VENDOR}, 1)
GOVENDORFLAG := -mod=vendor
endif

GOBUILD  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG) -trimpath $(GOVENDORFLAG)
GOBUILDNOVENDOR  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG) -trimpath
GOTEST   := CGO_ENABLED=1 $(GO) test -p $(P) --race
GOTESTNORACE := CGO_ENABLED=1 $(GO) test -p $(P)

ARCH  := "$(shell uname -s)"
LINUX := "Linux"
MAC   := "Darwin"
CDC_PKG := github.com/tikv/migration/cdc
PACKAGE_LIST := go list ./... | grep -vE 'vendor|proto|cdc\/tests|integration|testing_utils|pb|pbmock'
PACKAGES := $$($(PACKAGE_LIST))
FILES := $$(find . -name '*.go' -type f | grep -vE 'vendor|kv_gen|proto|pb\.go|pb\.gw\.go')
TEST_FILES := $$(find . -name '*_test.go' -type f | grep -vE 'vendor|kv_gen|integration|testing_utils')
FAILPOINT_DIR := $$(for p in $(PACKAGES); do echo $${p\#"github.com/tikv/migration/cdc/"}|grep -v "github.com/pingcap/migration/cdc/"; done)
FAILPOINT := tools/bin/failpoint-ctl

FAILPOINT_ENABLE  := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) enable >/dev/null)
FAILPOINT_DISABLE := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) disable >/dev/null)

RELEASE_VERSION =
ifeq ($(RELEASE_VERSION),)
	RELEASE_VERSION := v5.4.0-master
	release_version_regex := ^v5\..*$$
	release_branch_regex := "^release-[0-9]\.[0-9].*$$|^HEAD$$|^.*/*tags/v[0-9]\.[0-9]\..*$$"
	ifneq ($(shell git rev-parse --abbrev-ref HEAD | egrep $(release_branch_regex)),)
		# If we are in release branch, try to use tag version.
		ifneq ($(shell git describe --tags --dirty | egrep $(release_version_regex)),)
			RELEASE_VERSION := $(shell git describe --tags --dirty)
		endif
	else ifneq ($(shell git status --porcelain),)
		# Add -dirty if the working tree is dirty for non release branch.
		RELEASE_VERSION := $(RELEASE_VERSION)-dirty
	endif
endif

BUILDTS := $(shell date -u '+%Y-%m-%d %H:%M:%S')
GITHASH := $(shell git rev-parse HEAD)
GITBRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GOVERSION := $(shell go version)

# CDC LDFLAGS.
LDFLAGS += -X "$(CDC_PKG)/pkg/version.ReleaseVersion=$(RELEASE_VERSION)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GitHash=$(GITHASH)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GitBranch=$(GITBRANCH)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GoVersion=$(GOVERSION)"

default: build buildsucc

buildsucc:
	@echo Build TiKV CDC successfully!

all: dev install

dev: check test

test: unit_test

build: cdc

bank:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/bank ./tests/bank/bank.go ./tests/bank/case.go

build-failpoint: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc/main.go
	$(FAILPOINT_DISABLE)

cdc:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc/main.go

kafka_consumer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_kafka_consumer ./cmd/kafka-consumer/main.go

install:
	go install ./...

unit_test: check_failpoint_ctl
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	$(GOTEST) -cover -covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

unit_test_in_verify_ci: check_failpoint_ctl tools/bin/gotestsum tools/bin/gocov tools/bin/gocov-xml
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	CGO_ENABLED=1 tools/bin/gotestsum --junitfile cdc-junit-report.xml -- -v -timeout 5m -p $(P) --race \
	-covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	tools/bin/gocov convert "$(TEST_DIR)/cov.unit.out" | tools/bin/gocov-xml > cdc-coverage.xml
	$(FAILPOINT_DISABLE)
	@bash <(curl -s https://codecov.io/bash) -F cdc -f $(TEST_DIR)/cov.unit.out -t $(TICDC_CODECOV_TOKEN)

leak_test: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	$(GOTEST) -count=1 --tags leak $(PACKAGES) || { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

vet:
	@echo "vet"
	$(GO) vet $(PACKAGES) 2>&1 | $(FAIL_ON_STDOUT)

tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

# TODO: Unified cdc and dm config.
check-static: tools/bin/golangci-lint
	tools/bin/golangci-lint run --timeout 10m0s --skip-files kv_gen --skip-dirs tests

unit_test_coverage:
	grep -vE ".*.pb.go|$(CDC_PKG)/testing_utils/.*|$(CDC_PKG)/cdc/kv/testing.go|$(CDC_PKG)/cdc/sink/simple_mysql_tester.go|.*.__failpoint_binding__.go" "$(TEST_DIR)/cov.unit.out" > "$(TEST_DIR)/unit_cov.out"
	go tool cover -html "$(TEST_DIR)/unit_cov.out" -o "$(TEST_DIR)/unit_cov.html"
	go tool cover -func="$(TEST_DIR)/unit_cov.out"

data-flow-diagram: docs/data-flow.dot
	dot -Tsvg docs/data-flow.dot > docs/data-flow.svg

clean:
	go clean -i ./...
	rm -rf *.out
	rm -rf bin
	rm -rf tools/bin

install_test_python_dep:
	@echo "install python requirments for test"
	pip install --user -q -r ./dm/tests/requirements.txt

tools/bin/failpoint-ctl: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/failpoint-ctl github.com/pingcap/failpoint/failpoint-ctl

tools/bin/gocovmerge: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocovmerge github.com/zhouqiang-cl/gocovmerge

tools/bin/goveralls: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/goveralls github.com/mattn/goveralls

tools/bin/golangci-lint: tools/check/go.mod
	cd tools/check && $(GO) build  -mod=mod -o ../bin/golangci-lint github.com/golangci/golangci-lint/cmd/golangci-lint

tools/bin/mockgen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/mockgen github.com/golang/mock/mockgen

tools/bin/protoc-gen-gogofaster: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-gogofaster github.com/gogo/protobuf/protoc-gen-gogofaster

tools/bin/protoc-gen-grpc-gateway: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-grpc-gateway github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway

tools/bin/statik: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/statik github.com/rakyll/statik

tools/bin/gofumports: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gofumports mvdan.cc/gofumpt/gofumports

tools/bin/shfmt: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/shfmt mvdan.cc/sh/v3/cmd/shfmt

tools/bin/oapi-codegen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/oapi-codegen github.com/deepmap/oapi-codegen/cmd/oapi-codegen

tools/bin/gocov: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov  github.com/axw/gocov/gocov

tools/bin/gocov-xml: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov-xml github.com/AlekSi/gocov-xml

tools/bin/gotestsum: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gotestsum gotest.tools/gotestsum

tools/bin/errdoc-gen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/errdoc-gen github.com/pingcap/errors/errdoc-gen

check_failpoint_ctl: tools/bin/failpoint-ctl

failpoint-enable: check_failpoint_ctl
	$(FAILPOINT_ENABLE)

failpoint-disable: check_failpoint_ctl
	$(FAILPOINT_DISABLE)
