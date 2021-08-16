ARG BASE_IMAGE
ARG CERT_IMAGE

FROM $BASE_IMAGE AS base

ENV GOPATH "/go"
ENV GO111MODULE "on"
WORKDIR /src
COPY go.* ./
RUN go mod download

FROM base AS builder

RUN --mount=target=. \
            --mount=type=cache,target=/root/.cache/go-build \
            BUILD_DIR=/out make build

FROM $CERT_IMAGE AS certs
RUN apk add --no-cache ca-certificates
RUN update-ca-certificates

FROM busybox:stable-glibc

COPY --from=builder /out/banyand-server /banyand-server
COPY --from=certs /etc/ssl/certs /etc/ssl/certs

ENTRYPOINT ["/banyand-server"]