# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
# check=error=true

# Build stage
FROM golang:1.27.3-alpine3.24@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS builder

# Install necessary dependencies (zstd-dev used with cgo)
RUN apk --no-cache add --update make tzdata ca-certificates gcc musl-dev zstd-dev

# Enable CGO
ENV CGO_ENABLED=1

# Create app directory
RUN mkdir /app
WORKDIR /app

# Copy Go modules files and download dependencies
COPY go.mod go.sum ./
# go.mod is the single source of the Go version. The build fails when the builder runs another one.
RUN want="go$(awk '$1 == "go" { print $2; exit }' go.mod)"; have="$(GOTOOLCHAIN=local go env GOVERSION)"; \
    [ "$have" = "$want" ] || { echo "builder runs $have, go.mod wants $want" >&2; exit 1; }
RUN go mod download

ARG VERSION
ARG COMMIT_HASH
ARG BUILD_DATE
ARG REVISION

# Copy application source code and build binary
COPY . .
RUN go build \
    -ldflags="-s -w \
    -X 'github.com/rudderlabs/keydb/release.version=${VERSION}' \
    -X 'github.com/rudderlabs/keydb/release.commit=${COMMIT_HASH}' \
    -X 'github.com/rudderlabs/keydb/release.buildDate=${BUILD_DATE}' \
    -X 'github.com/rudderlabs/keydb/release.builtBy=${REVISION}'" \
    -o ./keydb ./cmd/node

# Final stage
FROM alpine:3.24.3@sha256:b3413572228e34bd1c9245f5565e6cd127005655e8a7899aef33d8e83255c9e6

# The version floor changes this instruction, so an old cached layer is not reused.
# apk fails here when the repository serves an older bundle.
RUN apk --no-cache upgrade && \
    apk --no-cache add tzdata curl bash zstd-libs 'ca-certificates-bundle>=20261020'

COPY --link --from=builder /app/keydb ./

# The node stays root: existing /data volumes are root-owned and the chart sets no fsGroup.
# A move to a non-root user needs an fsGroup in the operator chart first.
ENTRYPOINT ["/keydb"]
