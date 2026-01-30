# Define build arguments
# GO_VERSION is updated automatically to match go.mod, see Makefile
ARG PKG_NAME=github.com/rudderlabs/keydb

# Build stage
FROM golang:1.25.6-alpine3.22@sha256:fa3380ab0d73b706e6b07d2a306a4dc68f20bfc1437a6a6c47c8f88fe4af6f75 AS builder

# Install necessary dependencies (zstd-dev used with cgo)
RUN apk --no-cache add --update make tzdata ca-certificates gcc musl-dev zstd-dev

# Enable CGO
ENV CGO_ENABLED=1

# Create app directory
RUN mkdir /app
WORKDIR /app

# Copy Go modules files and download dependencies
COPY go.mod .
COPY go.sum .
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
FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659

# Update and install additional packages (zstd-libs used with cgo)
RUN apk --no-cache upgrade && \
    apk --no-cache add tzdata curl bash zstd-libs

# Copy built binary from previous stage
COPY --from=builder /app/keydb ./

# Specify default command to run when container starts
CMD ["/keydb"]
