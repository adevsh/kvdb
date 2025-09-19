# Makefile for key-value database REPL
# Purpose: Automate build, test, and deployment processes

BINARY_NAME=kvdb
DOCKER_IMAGE=kvdb:latest
CLUSTER_SIZE=3

.PHONY: all build test clean docker-build docker-run cluster

all: build

build:
		@echo "Building the key-value database REPL..."
		go build -o ${BINARY_NAME} ./cmd/main.go

test:
		@echo "Running tests..."
		go test -v ./..

clean:
		@echo "Cleaning up..."
		go clean
		rm -f ${BINARY_NAME}

docker-build:
		@echo "Building docker image..."
		docker build -t ${DOCKER_IMAGE} .

docker-run:
		@echo "Running docker container..."
		docker run -it --rm -p 8080:8080 ${DOCKER_IMAGE}

cluster:
		@echo "Starting $(CLUSTER_SIZE) node cluster..."
		@for i in $$(seq 1 $(CLUSTER_SIZE)); do \
				port=$$((8080 + $$i)); \
				raft_port=$$((9090 + $$i)); \
				data_dir="data/node$$i"; \
				mkdir -p $$data_dir; \
				echo "Starting node $$i on port $$port (raft: $$raft_port)"; \
				./$(BINARY_NAME) --id $$i --raft :$$raft_port --data-dir $$data_dir & \
		done

benchmark:
		@echo "Running benchmark..."
		go run ./cmd/benchmark

.PHONY: lint
lint:
		@echo "Running linter..."
		golangci-lint run

