// Package clilog builds the structured logger used by the engine's command
// binaries from -log-format / -log-level flags and installs it as
// slog.Default, so the server and connector libraries (which default to
// slog.Default) inherit the same handler.
package clilog

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Flags registers -log-format and -log-level on fs and returns a function that
// builds the configured logger, installs it via slog.SetDefault, and returns
// it. Call the returned function after flag.Parse.
//
//	build := clilog.Flags(flag.CommandLine)
//	flag.Parse()
//	logger := build()
func Flags(fs *flag.FlagSet) func() *slog.Logger {
	format := fs.String("log-format", "text", "log format: text or json")
	level := fs.String("log-level", "info", "log level: debug, info, warn or error")
	return func() *slog.Logger {
		logger := slog.New(handler(*format, parseLevel(*level)))
		slog.SetDefault(logger)
		return logger
	}
}

func handler(format string, level slog.Level) slog.Handler {
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(format, "json") {
		return slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.NewTextHandler(os.Stderr, opts)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
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

// Fatal logs msg at error level (attaching err when non-nil) and exits with a
// non-zero status. Use for terminal startup failures in command binaries.
func Fatal(logger *slog.Logger, msg string, err error) {
	if err != nil {
		logger.Error(msg, slog.Any("err", err))
	} else {
		logger.Error(msg)
	}
	os.Exit(1)
}

// Fatalf is Fatal with a formatted message and no error attribute.
func Fatalf(logger *slog.Logger, format string, args ...any) {
	logger.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}
