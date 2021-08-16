ARG base_image
ARG cert_image

FROM $base_image AS base

ENV GOPATH "/go"
ENV GO111MODULE "on"
WORKDIR /src
COPY go.* ./
RUN go mod download

FROM base AS builder

RUN --mount=target=. \
            --mount=type=cache,target=/root/.cache/go-build \
            go build -o /out/banyand-server .

FROM $cert_image AS certs
RUN apk add --no-cache ca-certificates
RUN update-ca-certificates

FROM busybox:stable-glibc

COPY --from=builder /out/banyand-server /banyand-server
COPY --from=certs /etc/ssl/certs /etc/ssl/certs

ENTRYPOINT ["/banyand-server"]