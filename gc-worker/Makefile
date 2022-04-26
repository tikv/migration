# Build pd-server, pd-ctl, pd-recover
default: build

GO             := GO111MODULE=on go
BUILD_BIN_PATH := $(shell pwd)/bin
PACKAGES       := go list ./...
DIRECTORIES    := $(PACKAGES) | sed 's|github.com/tikv/migration/gc-worker/||'

#### Build ####
build: gc-worker

gc-worker: 
	CGO_ENABLED=1 $(GO) build -tags codes -ldflags '$(LDFLAGS)' -o $(BUILD_BIN_PATH)/gc-worker cmd/*.go

check: check/golangci-lint check/gosec

check/golangci-lint: tools/bin/golangci-lint
	GO111MODULE=on CGO_ENABLED=0 tools/bin/golangci-lint run -v $$($(DIRECTORIES)) --config ../.golangci.yml --timeout 5m

tools/bin/golangci-lint:
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./tools/bin v1.41.1

check/gosec:
	$(GO) install github.com/securego/gosec/v2/cmd/gosec@v2.9.1
	gosec -fmt=junit-xml -out=results.xml -stdout -verbose=text -exclude=G103,G104,G204,G304,G307,G401,G404,G501,G505,G601 ./...

#### Clean up ####

clean: clean-build

clean-build:
	# Cleaning building files...
	rm -rf $(BUILD_BIN_PATH)
	rm -rf tools/*

.PHONY: clean clean-build
