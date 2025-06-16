GO := go
DOCKER_USER := 

# go tools versions
protoc-gen-go=google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
protoc-gen-go-grpc=google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

.PHONY: install-tools
install-tools:
	$(GO) install $(protoc-gen-go)
	$(GO) install $(protoc-gen-go-grpc)

# Generate protobuf files
.PHONY: proto
proto: install-tools
	@echo "Generating protobuf files..."
	protoc --go_out=paths=source_relative:. proto/*.proto
	protoc --go-grpc_out=paths=source_relative:. proto/*.proto

build:
	@if [ -z "$(DOCKER_USER)" ]; then \
		echo "Error: DOCKER_USER variable is empty"; \
		exit 1; \
	fi
	docker build -t $(DOCKER_USER)/keydb:latest .
	docker push $(DOCKER_USER)/keydb:latest
