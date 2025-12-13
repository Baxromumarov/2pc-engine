BIN_DIR ?= bin

.PHONY: all build build-cli build-master build-node test clean

all: build

build: build-cli build-master build-node

build-cli: $(BIN_DIR)
	go build -o $(BIN_DIR)/cli ./cmd/cli

build-master: $(BIN_DIR)
	go build -o $(BIN_DIR)/master ./cmd/master

build-node: $(BIN_DIR)
	go build -o $(BIN_DIR)/node ./cmd/node

test:
	go test ./...

clean:
	rm -rf $(BIN_DIR)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)
