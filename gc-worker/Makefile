# Copyright 2022 PingCAP, Inc.
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

default: build

GO             := GO111MODULE=on go
BUILD_BIN_PATH := $(shell pwd)/bin
PACKAGES       := go list ./...
DIRECTORIES    := $(PACKAGES) | sed 's|github.com/tikv/migration/gc-worker/||'
TEST_PARALLEL  := 8

GCWORKER_PKG   := github.com/tikv/migration/gc-worker

LDFLAGS += -X "$(GCWORKER_PKG)/server.GCWorkerVersion=$(shell git describe --tags --dirty --always)"
#### Build ####
build: gc-worker

gc-worker: 
	$(GO) build -tags codes -gcflags "all=-N -l" -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/gc-worker cmd/*.go
 
check: check/golangci-lint check/gosec

check/golangci-lint: tools/bin/golangci-lint
	GO111MODULE=on CGO_ENABLED=0 tools/bin/golangci-lint run -v $$($(DIRECTORIES)) --config ../.golangci.yml --timeout 5m

tools/bin/golangci-lint:
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./tools/bin v1.41.1

check/gosec:
	$(GO) install github.com/securego/gosec/v2/cmd/gosec@v2.9.1
	gosec -fmt=junit-xml -out=results.xml -stdout -verbose=text -exclude=G103,G104,G204,G304,G307,G401,G404,G501,G505,G601 ./...

test:
	$(GO) test -p $(TEST_PARALLEL) -race -ldflags '$(LDFLAGS)' -tags leak $$($(PACKAGES))

#### Clean up ####

clean: clean-build

clean-build:
	# Cleaning building files...
	rm -rf $(BUILD_BIN_PATH)
	rm -rf tools/*

.PHONY: clean clean-build
