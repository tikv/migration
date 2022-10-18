FROM golang:1.18.0-alpine3.15 as builder
RUN apk add --no-cache git make bash
WORKDIR /go/src/github.com/tikv/migration/br
COPY . .
RUN make release

FROM alpine:3.15
RUN apk add --no-cache tzdata bash curl socat
COPY --from=builder /go/src/github.com/tikv/migration/br/bin/tikv-br /usr/bin/
ENTRYPOINT ["/bin/bash"]
