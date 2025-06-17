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

build-operator:
	@if [ -z "$(DOCKER_USER)" ]; then \
		echo "Error: DOCKER_USER variable is empty"; \
		exit 1; \
	fi
	docker build -t $(DOCKER_USER)/keydb-operator:latest -f Dockerfile-operator .
	docker push $(DOCKER_USER)/keydb-operator:latest

run:
	@if [ -z "$(DOCKER_USER)" ]; then \
		echo "Error: DOCKER_USER variable is empty"; \
		exit 1; \
	fi
	docker run --rm -it -p 50051:50051 \
		-e KEYDB_PORT=50051 \
		-e KEYDB_BADGERDB_DEDUP_COMPRESS=true \
		$(DOCKER_USER)/keydb:latest

deploy:
	@if [ -z "$(NAMESPACE)" ]; then \
		echo "Error: NAMESPACE variable is empty"; \
		exit 1; \
	fi
	@if [ -z "$(DOCKER_USER)" ]; then \
		echo "Error: DOCKER_USER variable is empty"; \
		exit 1; \
	fi
	helm upgrade --install keydb-operator ./helm/keydb-operator \
		--namespace $(NAMESPACE) \
		--set image.repository=$(DOCKER_USER)/keydb-operator
