package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/blaspat/flare/internal/cmd"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := cmd.ParseAndRun(ctx, os.Args); err != nil {
		log.Fatal(err)
	}
}
