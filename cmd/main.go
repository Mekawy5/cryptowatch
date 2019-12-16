package main

import "github.com/Mekawy5/cryptowatch/pkg/worker"

func main() {
	p := worker.NewProcessor()
	p.Process()
}
