package logger

import (
	"log/slog"
	"os"
	"strings"
)

// L is the package-level structured logger used across the broker.
var L *slog.Logger

func init() {
	L = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: parseLevel(os.Getenv("LOG_LEVEL")),
	}))
}

// parseLevel converts a string like "debug", "info", "warn", "error" to slog.Level.
// Defaults to Info.
func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
