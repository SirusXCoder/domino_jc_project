package telemetry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
)

const TraceIDHeader = "X-Trace-ID"

type traceIDKey struct{}

var (
	defaultLogger *slog.Logger
	// AppLogger is the process-wide structured logger set by InitLogger.
	AppLogger *slog.Logger
)

// InitLogger configures structured JSON logging on stdout and sets the process default.
func InitLogger() *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	defaultLogger = logger
	AppLogger = logger
	return logger
}

// DefaultLogger returns the logger installed by InitLogger, or slog.Default().
func DefaultLogger() *slog.Logger {
	if defaultLogger != nil {
		return defaultLogger
	}
	return slog.Default()
}

// WithTraceID attaches a trace identifier to ctx for downstream structured logs.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

// TraceIDFromContext reads the trace identifier previously stored on ctx.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey{}).(string); ok {
		return v
	}
	return ""
}

// LoggerFromContext returns a logger enriched with trace_id when present on ctx.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	return LoggerWithTrace(DefaultLogger(), ctx)
}

// LoggerWithTrace returns logger with trace_id from ctx when available.
func LoggerWithTrace(logger *slog.Logger, ctx context.Context) *slog.Logger {
	if tid := TraceIDFromContext(ctx); tid != "" {
		return logger.With("trace_id", tid)
	}
	return logger
}

// TraceMiddleware propagates or generates X-Trace-ID on every HTTP request.
func TraceMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := r.Header.Get(TraceIDHeader)
		if traceID == "" {
			traceID = newTraceID()
		}
		w.Header().Set(TraceIDHeader, traceID)
		ctx := WithTraceID(r.Context(), traceID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func newTraceID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b)
}
