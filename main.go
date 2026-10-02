package main

import (
	"log/slog"
	"os"
)

func main() {

	//slog.New() creates a new logger
	//Takes a handler to determine how log messages are formatted and where they are sent
	//slog.NewTextHandle creates a handler that formats output logs as readable text
	//&slog.HandlerOptions specifies logging configuration (minimum log level etc)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("FairGate starting",
		"Version", "0.0.1",
		"Status", "Initializing",
	)
}
