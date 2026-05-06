// Package log — slog logger с дефолтами курса.
//
// Уровень читается из LOG_LEVEL (debug/info/warn/error), хэндлер — текстовый
// в stderr, чтобы stdout оставался чистым для пайплайнов (например, чтобы
// можно было kafka-console-producer ... | go run ./cmd/consumer).
package log

import (
	"log/slog"
	"os"
	"strings"
)

// New возвращает *slog.Logger с уровнем из LOG_LEVEL (info по умолчанию).
func New() *slog.Logger {
	level := parseLevel(os.Getenv("LOG_LEVEL"))
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	return slog.New(h)
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
