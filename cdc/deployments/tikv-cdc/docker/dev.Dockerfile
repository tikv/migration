FROM golang:1.21-alpine3.18 as builder
RUN apk add --no-cache git make bash
WORKDIR /go/src/github.com/tikv/migration/cdc
COPY . .
ENV CDC_ENABLE_VENDOR=1
RUN go mod vendor
RUN make failpoint-enable
RUN make
RUN make failpoint-disable

FROM alpine:3.18
RUN apk add --no-cache tzdata bash curl socat
COPY --from=builder /go/src/github.com/tikv/migration/cdc/bin/tikv-cdc /usr/bin/
# TiKV-CDC use TiCDC operator to run TiKV-CDC server, TiCDC operator will use '/cdc' to start server 
COPY --from=builder /go/src/github.com/tikv/migration/cdc/bin/tikv-cdc /cdc 
EXPOSE 8600
CMD [ "tikv-cdc" ]
