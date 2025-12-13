#!/bin/bash
# Script to add a new node to the 2PC cluster in production
#
# Usage: ./add-node.sh <node-address> <master-address> <database-dsn>
#
# Example:
#   ./add-node.sh node3:8080 master:8080 "postgres://user:pass@db:5432/shard3"

set -e

NODE_ADDR=${1:-""}
MASTER_ADDR=${2:-"localhost:8080"}
DATABASE_DSN=${3:-""}

if [ -z "$NODE_ADDR" ]; then
    echo "Usage: $0 <node-address> [master-address] [database-dsn]"
    echo ""
    echo "Steps to add a new node:"
    echo "  1. Provision a new database for the node (e.g., shard3)"
    echo "  2. Start the node process with the database DSN"
    echo "  3. Run this script to register the node with the master"
    echo ""
    echo "Example:"
    echo "  # On the new node server:"
    echo "  POSTGRES_DSN='postgres://user:pass@db:5432/shard3' ./node --addr=node3:8080 --nodes=master:8080"
    echo ""
    echo "  # Then register with master:"
    echo "  $0 node3:8080 master:8080"
    exit 1
fi

echo "=== Adding node $NODE_ADDR to cluster ==="

# Step 1: Check if the node is reachable
echo "Checking if node $NODE_ADDR is reachable..."
if ! curl -s --connect-timeout 5 "http://$NODE_ADDR/health" > /dev/null 2>&1; then
    echo "ERROR: Cannot reach node at $NODE_ADDR"
    echo "Make sure the node is running with:"
    echo "  POSTGRES_DSN='<your-dsn>' ./node --addr=$NODE_ADDR --nodes=$MASTER_ADDR"
    exit 1
fi
echo "✓ Node is reachable"

# Step 2: Check if master is reachable
echo "Checking if master $MASTER_ADDR is reachable..."
if ! curl -s --connect-timeout 5 "http://$MASTER_ADDR/health" > /dev/null 2>&1; then
    echo "ERROR: Cannot reach master at $MASTER_ADDR"
    exit 1
fi
echo "✓ Master is reachable"

# Step 3: Register the node with the master
echo "Registering node with master..."
RESPONSE=$(curl -s -X POST "http://$MASTER_ADDR/cluster/join" \
    -H "Content-Type: application/json" \
    -d "{\"address\":\"$NODE_ADDR\"}")

if echo "$RESPONSE" | grep -q '"success":true'; then
    echo "✓ Node registered successfully!"
    echo ""
    echo "Cluster nodes:"
    curl -s "http://$MASTER_ADDR/cluster/nodes" | jq .
else
    echo "ERROR: Failed to register node"
    echo "$RESPONSE" | jq .
    exit 1
fi

echo ""
echo "=== Node $NODE_ADDR successfully added to cluster ==="
echo ""
echo "The node will now participate in 2PC transactions."
echo "Test with:"
echo "  curl -X POST http://$MASTER_ADDR/transaction \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"payload\":{\"table\":\"users\",\"operation\":\"update\",\"values\":{\"balance\":100},\"where\":{\"id\":1}}}'"
