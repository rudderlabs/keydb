# Define build arguments
# GO_VERSION is updated automatically to match go.mod, see Makefile
ARG PKG_NAME=github.com/rudderlabs/keydb

# Build stage
FROM golang:1.25.4-alpine3.22@sha256:d2ede9f3341a67413127cf5366bb25bbad9b0a66e8173cae3a900ab00e84861f AS builder

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
FROM alpine:3.22.2@sha256:4b7ce07002c69e8f3d704a9c5d6fd3053be500b7f1c69fc0d80990c2ad8dd412

# Update and install additional packages (zstd-libs used with cgo)
RUN apk --no-cache upgrade && \
    apk --no-cache add tzdata curl bash zstd-libs

# Copy built binary from previous stage
COPY --from=builder /app/keydb ./

# Specify default command to run when container starts
CMD ["/keydb"]
