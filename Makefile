BIN_DIR ?= bin
LOG_DIR ?= logs
PIDS_FILE ?= .cluster.pids

# Default addresses
MASTER_ADDR ?= localhost:8080
NODE1_ADDR ?= localhost:8081
NODE2_ADDR ?= localhost:8082
NODES := $(MASTER_ADDR),$(NODE1_ADDR),$(NODE2_ADDR)
PORTS := $(shell echo $(MASTER_ADDR) $(NODE1_ADDR) $(NODE2_ADDR) | tr ' ' '\n' | sed 's/.*://' | sort -u | tr '\n' ' ')

# Default DSNs (override in env or .env)
POSTGRES_DSN_MASTER ?= postgres://postgres:postgres@localhost:5432/test?sslmode=disable
POSTGRES_DSN_SLAVE1 ?= postgres://postgres:postgres@localhost:5432/slave1?sslmode=disable
POSTGRES_DSN_SLAVE2 ?= postgres://postgres:postgres@localhost:5432/slave2?sslmode=disable

.PHONY: all build build-cli build-master build-node test clean start-cluster stop-cluster

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

start-cluster: stop-cluster
	@mkdir -p $(LOG_DIR)
	@rm -f $(PIDS_FILE)
	@echo "Starting master on $(MASTER_ADDR) ..."
	@POSTGRES_DSN=$(POSTGRES_DSN_MASTER) go run ./cmd/master --addr=$(MASTER_ADDR) --nodes=$(NODES) > $(LOG_DIR)/master.log 2>&1 & echo $$! >> $(PIDS_FILE)
	@echo "Starting node1 on $(NODE1_ADDR) ..."
	@POSTGRES_DSN=$(POSTGRES_DSN_SLAVE1) go run ./cmd/node --addr=$(NODE1_ADDR) --nodes=$(NODES) > $(LOG_DIR)/node1.log 2>&1 & echo $$! >> $(PIDS_FILE)
	@echo "Starting node2 on $(NODE2_ADDR) ..."
	@POSTGRES_DSN=$(POSTGRES_DSN_SLAVE2) go run ./cmd/node --addr=$(NODE2_ADDR) --nodes=$(NODES) > $(LOG_DIR)/node2.log 2>&1 & echo $$! >> $(PIDS_FILE)
	@sleep 2
	@echo "Cluster started. Logs in $(LOG_DIR). Stop with: make stop-cluster"

stop-cluster:
	@if [ -f $(PIDS_FILE) ]; then \
		echo "Stopping cluster..."; \
		xargs kill < $(PIDS_FILE) >/dev/null 2>&1 || true; \
		rm -f $(PIDS_FILE); \
		rm -f $(LOG_DIR)/*.log; \
	else \
		echo "No cluster processes to stop."; \
	fi
	@ports="$(PORTS)"; \
	if [ -n "$$ports" ]; then \
		echo "Killing any listeners on ports: $$ports"; \
		for p in $$ports; do \
			lsof -ti :$$p 2>/dev/null | xargs kill >/dev/null 2>&1 || true; \
		done; \
	fi

$(BIN_DIR):
	mkdir -p $(BIN_DIR)
