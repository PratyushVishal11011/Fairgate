package main

import (
	"FairGate/internal/server"
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

	//Start Fairgate on port 9001 and the server will keep it running while it waits for a client to connect.
	if err := server.Run(":9001", logger); err != nil {
		logger.Error("TCP server stopped", "error", err)
		os.Exit(1)
	}
}
