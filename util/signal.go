package util

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var StopSignals = []os.Signal{syscall.SIGTERM, os.Interrupt}

func CancelSignals(ctx context.Context, cancelContext context.CancelFunc, signals ...os.Signal) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, signals...)
	go func() {
		select {
		case <-ctx.Done():
			break
		case s := <-signalChan:
			log.Printf("Captured %v. Exiting...", s)
			cancelContext()
			signal.Reset(signals...)
		}
	}()
}
