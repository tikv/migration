# Build pd-server, pd-ctl, pd-recover
default: build

#### Build ####

BUILD_BIN_PATH := $(shell pwd)/bin

build: gc-worker

gc-worker: 
	go build $(BUILD_FLAGS) -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -tags "$(BUILD_TAGS)" -o $(BUILD_BIN_PATH)/gc-worker cmd/main.go

#### Clean up ####

clean: clean-build

clean-build:
	# Cleaning building files...
	rm -rf $(BUILD_BIN_PATH)

.PHONY: clean clean-build
