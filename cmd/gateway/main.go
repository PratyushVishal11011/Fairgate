package main

import (
	"FairGate/internal/config"
	"FairGate/internal/server"
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	//the config file is optional: -config wins over FAIRGATE_CONFIG, and without either the built-in defaults apply.
	//environment variables such as CLICKHOUSE_PASSWORD still override values from the file.
	configPath := flag.String("config", os.Getenv("FAIRGATE_CONFIG"), "path to a FairGate YAML config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("FairGate failed to load its configuration", "error", err)
		os.Exit(1)
	}

	//Validate has already checked the level.
	level, _ := cfg.LogLevel()

	//slog.New() creates a new logger
	//Takes a handler to determine how log messages are formatted and where they are sent
	//slog.NewTextHandle creates a handler that formats output logs as readable text
	//&slog.HandlerOptions specifies logging configuration (minimum log level etc)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	//Start Fairgate on the configured address and the server will keep it running while it waits for a client to connect.
	if err := run(logger, cfg, *configPath); err != nil {
		logger.Error("FairGate stopped with an error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, cfg config.Config, configPath string) error {
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
		"Config", configPath,
	)

	if err := server.RunConfig(ctx, cfg, logger); err != nil {
		return err
	}

	logger.Info("FairGate shut down cleanly")
	return nil
}
