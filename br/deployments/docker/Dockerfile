FROM golang:1.21-alpine3.18 as builder
RUN apk add --no-cache git make bash build-base
WORKDIR /go/src/github.com/tikv/migration/br
COPY . .
RUN make release

FROM alpine:3.18
RUN apk add --no-cache tzdata bash curl socat
COPY --from=builder /go/src/github.com/tikv/migration/br/bin/tikv-br /usr/bin/
ENTRYPOINT ["/bin/bash"]
