FROM golang:1.21-alpine3.18 as builder
RUN apk add --no-cache git make bash
WORKDIR /go/src/github.com/tikv/migration/cdc

COPY go.mod .
COPY go.sum .
RUN GO111MODULE=on go mod download

COPY . .
ENV CDC_ENABLE_VENDOR=0
RUN make kafka_consumer

FROM alpine:3.18
RUN apk add --no-cache tzdata bash curl socat kafkacat
COPY --from=builder /go/src/github.com/tikv/migration/cdc/bin/cdc_kafka_consumer /cdc_kafka_consumer
CMD [ "/cdc_kafka_consumer" ]
