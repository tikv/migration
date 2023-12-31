### Makefile for TiKV-CDC tools

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

shellcheck: tools/bin/shellcheck
	tools/bin/shellcheck ./**/*.sh

tools/bin/shellcheck:
	wget -qO- "https://github.com/koalaman/shellcheck/releases/download/$(SCVERSION)/shellcheck-$(SCVERSION).$(ARCH).x86_64.tar.xz" | tar -xJv
	mv "shellcheck-$(SCVERSION)/shellcheck" tools/bin/
	chmod +x tools/bin/shellcheck
	rm -rf "shellcheck-$(SCVERSION)"

tools/bin/msgp: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/msgp github.com/tinylib/msgp
