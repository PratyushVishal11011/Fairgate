package main

import (
	"FairGate/internal/server"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	//slog.New() creates a new logger
	//Takes a handler to determine how log messages are formatted and where they are sent
	//slog.NewTextHandle creates a handler that formats output logs as readable text
	//&slog.HandlerOptions specifies logging configuration (minimum log level etc)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	//Start Fairgate on port 9001 and the server will keep it running while it waits for a client to connect.
	if err := run(logger); err != nil {
		logger.Error("FairGate stopped with an error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	//create a signal aware context
	//context.Background() creates an empty parent context -> it has no deadline, cancellation signal or associated error initially
	//system.SIGINT -> generated when ctrl + c is pressed in the terminal
	//system.SIGTERM -> when kill pid is called
	//system.SIGKILL -> immediate shutdown, graceful is impossible
	//returns 2 values; ctx and sto
	//ctx is passed to the server so that it can detect stops
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	//stop unregisters the signal notification and releases resources associated with it.
	//when one of these signal arrives, ctx.Done() is closed; allowing goroutines to react
	//defer schedules stop() to run when run() returns
	//this releases the resources used by signal.NotifyContext and unregisters its signal handling.
	defer stop()
	logger.Info("FairGate starting",
		"Version", "0.1.0",
		"Status", "Initializing",
	)

	if err := server.RunContext(ctx, ":9001", logger); err != nil {
		return err
	}

	logger.Info("FairGate shut down cleanly")
	return nil
}
