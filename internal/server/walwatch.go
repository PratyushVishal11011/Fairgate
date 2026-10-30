package server

import (
	"context"
	"log/slog"
	"time"
)

// watchWAL polls health and calls expire once the WAL has been degraded for exitAfter without a break.
func watchWAL(ctx context.Context, logger *slog.Logger, health func() error, exitAfter, poll time.Duration, expire func(error)) {
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	var since time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			err := health()
			switch {
			case err == nil && !since.IsZero():
				logger.Info("WAL accepting appends again", "degraded_for", now.Sub(since))
				since = time.Time{}
			case err == nil:
			case since.IsZero():
				since = now
				logger.Error("WAL degraded; refusing events until a fresh segment works",
					"error", err,
					"exit_after", exitAfter,
				)
			case now.Sub(since) >= exitAfter:
				logger.Error("WAL still degraded; stopping the gateway so it can be restarted",
					"error", err,
					"degraded_for", now.Sub(since),
				)
				expire(err)
				return
			}
		}
	}
}
