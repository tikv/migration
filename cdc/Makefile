### Makefile for TiKV-CDC
.PHONY: build test check clean fmt cdc release debug

PROJECT=migration/cdc
P=3

FAIL_ON_STDOUT := awk '{ print  } END { if (NR > 0) { exit 1  }  }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(CURDIR)/bin:$(path_to_add):$(PATH)

SHELL := /usr/bin/env bash

TEST_DIR := $(shell mktemp -d -t tikv_cdc_test.XXXXXXXX)

GO       := GO111MODULE=on go
ifeq (${CDC_ENABLE_VENDOR}, 1)
GOVENDORFLAG := -mod=vendor
endif

BUILD_FLAG := -buildvcs=false

GOBUILD  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG) -trimpath $(GOVENDORFLAG)
GOBUILD_DEBUG  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG) -gcflags "all=-N -l" $(GOVENDORFLAG)
GOBUILDNOVENDOR  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG) -trimpath
GOTEST   := CGO_ENABLED=1 $(GO) test -p $(P) --race
GOTESTNORACE := CGO_ENABLED=1 $(GO) test -p $(P)

ARCH  := "$(shell uname -s)"
LINUX := "Linux"
MAC   := "Darwin"
CDC_PKG := github.com/tikv/migration/cdc
# Can use `PACKAGE_PATH=<package path> make test` to test specified package. E.g, `PACKAGE_PATH=cdc/sink make test`.
PACKAGE_PATH ?= ...
PACKAGE_LIST := go list $(CDC_PKG)/$(PACKAGE_PATH) | grep -vE 'vendor|proto|cdc\/tests|integration|testing_utils|pb|pbmock'
PACKAGES := $$($(PACKAGE_LIST))
FILES := $$(find . -name '*.go' -type f | grep -vE 'vendor|kv_gen|proto|pb\.go|pb\.gw\.go')
TEST_FILES := $$(find . -name '*_test.go' -type f | grep -vE 'vendor|kv_gen|integration|testing_utils')
FAILPOINT_DIR := $$(for p in $(PACKAGES); do echo $${p\#"github.com/tikv/migration/cdc/"}|grep -v "github.com/tikv/migration/cdc/"; done)
FAILPOINT := tools/bin/failpoint-ctl

FAILPOINT_ENABLE  := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) enable >/dev/null)
FAILPOINT_DISABLE := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) disable >/dev/null)

RELEASE_VERSION ?=
ifeq ($(RELEASE_VERSION),)
	RELEASE_VERSION := v1.2-master
	release_version_regex := ^cdc-v[0-9]\..*$$
	release_branch_regex := "^cdc-[0-9]\.[0-9].*$$|^HEAD$$|^.*/*tags/cdc-v[0-9]\.[0-9]\..*$$"
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

SCVERSION := stable

default: build buildsucc

buildsucc:
	@echo Build TiKV CDC successfully!

all: dev install

dev: check test

test: unit_test

build: cdc

# "release" target is to meet release procedure requirement.
release: cdc

bank:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/bank ./tests/bank/bank.go ./tests/bank/case.go

build-failpoint: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/tikv-cdc ./cmd/cdc/main.go
	$(FAILPOINT_DISABLE)

cdc:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/tikv-cdc ./cmd/cdc/main.go

debug:
	$(GOBUILD_DEBUG) -ldflags '$(LDFLAGS)' -o bin/tikv-cdc ./cmd/cdc/main.go

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

unit_test_in_verify_ci: check_failpoint_ctl gotestsum gocov gocov-xml
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	CGO_ENABLED=1 gotestsum --junitfile cdc-junit-report.xml -- -v -timeout 30m -p $(P) --race \
	-covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	gocov convert "$(TEST_DIR)/cov.unit.out" | gocov-xml > cdc-coverage.xml
	$(FAILPOINT_DISABLE)
	@bash <(curl -s https://codecov.io/bash) -F cdc -f $(TEST_DIR)/cov.unit.out -t $(TIKV_MIGRATION_CODECOV_TOKEN)

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

check-copyright:
	@echo "check-copyright"
	@./scripts/check-copyright.sh

# TODO: Unified cdc and dm config.
check-static: tools/bin/golangci-lint
	tools/bin/golangci-lint run --timeout 10m0s --skip-files kv_gen --skip-dirs tests

fmt: gofumpt shfmt
	@echo "gofmt (simplify)"
	gofumpt -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)
	@echo "run shfmt"
	shfmt -d -w .

errdoc: tools/bin/errdoc-gen
	@echo "generator errors.toml"
	./tools/check/check-errdoc.sh

check-leaktest-added: gofumpt
	@echo "check leak test added in all unit tests"
	./scripts/add-leaktest.sh $(TEST_FILES)

check-merge-conflicts:
	@echo "check-merge-conflicts"
	@./scripts/check-merge-conflicts.sh

check-gosec: tools/bin/gosec
	tools/bin/gosec -fmt=junit-xml -out=results.xml -stdout -verbose=text -exclude-dir tests/.unmodify -exclude=G103,G104,G204,G304,G307,G401,G404,G501,G505,G601 ./...

check: check-copyright fmt check-static tidy errdoc check-leaktest-added check-merge-conflicts

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
	rm -rf scripts/bin

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
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b tools/bin v1.55.2

tools/bin/mockgen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/mockgen github.com/golang/mock/mockgen

tools/bin/protoc-gen-gogofaster: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-gogofaster github.com/gogo/protobuf/protoc-gen-gogofaster

tools/bin/protoc-gen-grpc-gateway: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-grpc-gateway github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway

tools/bin/statik: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/statik github.com/rakyll/statik

gofumpt:
	$(GO) install mvdan.cc/gofumpt@latest

shfmt:
	$(GO) install mvdan.cc/sh/v3/cmd/shfmt@latest

tools/bin/oapi-codegen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/oapi-codegen github.com/deepmap/oapi-codegen/cmd/oapi-codegen

gocov:
	$(GO) install github.com/axw/gocov/gocov@latest

gocov-xml:
	$(GO) install github.com/AlekSi/gocov-xml@latest

gotestsum:
	$(GO) install gotest.tools/gotestsum@latest

tools/bin/errdoc-gen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/errdoc-gen github.com/pingcap/errors/errdoc-gen

tools/bin/gosec:
	curl -sfL https://raw.githubusercontent.com/securego/gosec/master/install.sh | sh -s -- -b tools/bin v2.18.2

check_failpoint_ctl: tools/bin/failpoint-ctl

failpoint-enable: check_failpoint_ctl
	$(FAILPOINT_ENABLE)

failpoint-disable: check_failpoint_ctl
	$(FAILPOINT_DISABLE)

rawkv_data:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/rawkv_data ./tests/utils/rawkv_data/

cdc_state_checker:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_state_checker ./tests/utils/cdc_state_checker/

integration_test_build: check_failpoint_ctl rawkv_data cdc_state_checker
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
        -coverpkg=github.com/tikv/migration/... \
        -o bin/tikv-cdc.test github.com/tikv/migration/cdc/cmd/cdc \
    || { $(FAILPOINT_DISABLE); exit 1; }
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/tikv-cdc ./cmd/cdc/main.go \
    || { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

integration_test: prepare_test_binaries check_third_party_binary integration_test_build
	tests/integration_tests/run.sh "$(CASE)"
	@bash <(curl -s https://codecov.io/bash) -F cdc -s /tmp/tikv_cdc_test -f *.out -t $(TIKV_MIGRATION_CODECOV_TOKEN)

# To make try on integration tests easier.
# Comment out the succeed cases and retry from the failed one.
# TODO: find better method.
integration_test_by_group: prepare_test_binaries check_third_party_binary integration_test_build
	tests/integration_tests/run_group.sh G00
	tests/integration_tests/run_group.sh G01
	tests/integration_tests/run_group.sh G02
	tests/integration_tests/run_group.sh G03
	tests/integration_tests/run_group.sh G04
	tests/integration_tests/run_group.sh G05
	tests/integration_tests/run_group.sh G06
	tests/integration_tests/run_group.sh G07
	tests/integration_tests/run_group.sh G08
	tests/integration_tests/run_group.sh G09
	tests/integration_tests/run_group.sh G10
	tests/integration_tests/run_group.sh G11
	tests/integration_tests/run_group.sh G12
	tests/integration_tests/run_group.sh others

prepare_test_binaries:
	cd scripts && ./download-integration-test-binaries.sh master && cd ..
	touch prepare_test_binaries

check_third_party_binary:
	@which scripts/bin/tidb-server
	@which scripts/bin/tikv-server
	@which scripts/bin/pd-server
	@which scripts/bin/pd-ctl
	@which scripts/bin/go-ycsb
	@which scripts/bin/etcdctl

shellcheck: tools/bin/shellcheck
	tools/bin/shellcheck ./**/*.sh

tools/bin/shellcheck:
	wget -qO- "https://github.com/koalaman/shellcheck/releases/download/$(SCVERSION)/shellcheck-$(SCVERSION).$(ARCH).x86_64.tar.xz" | tar -xJv
	mv "shellcheck-$(SCVERSION)/shellcheck" tools/bin/
	chmod +x tools/bin/shellcheck
	rm -rf "shellcheck-$(SCVERSION)"
