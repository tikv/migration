FROM golang:1.18.0-alpine3.15 as builder
RUN apk add --no-cache git make bash
WORKDIR /go/src/github.com/tikv/migration/cdc
COPY . .
ENV CDC_ENABLE_VENDOR=0
RUN make

FROM alpine:3.15
RUN apk add --no-cache tzdata bash curl socat
COPY --from=builder /go/src/github.com/tikv/migration/cdc/bin/tikv-cdc /usr/bin/
# Tikv-cdc use ticdc operator to run tikv-cdc server, ticdc operator will use '/cdc' to start server 
COPY --from=builder /go/src/github.com/tikv/migration/cdc/bin/tikv-cdc /cdc 
EXPOSE 8600
CMD [ "tikv-cdc" ]
