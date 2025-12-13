package main

import (
	"fmt"
)

func main() {
	fmt.Println("2PC Engine - Distributed Two-Phase Commit System")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("  Start a node:   go run ./cmd/node --addr=localhost:8081")
	fmt.Println("  Start master:   go run ./cmd/master --addr=localhost:8080 --nodes=localhost:8081,localhost:8082")
	fmt.Println("  CLI tool:       go run ./cmd/cli <command>")
	fmt.Println("")
	fmt.Println("CLI Commands:")
	fmt.Println("  commit --master=<addr> --payload='{...}'  - Execute a 2PC transaction")
	fmt.Println("  health --addr=<addr>                      - Check node health")
	fmt.Println("  status --nodes=<node1,node2,...>          - Show cluster status")
}
