package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sony/gobreaker"

	"domino_jc_project/pkg/api"
	"domino_jc_project/pkg/database"
	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/repository"
	"domino_jc_project/pkg/resilience"
	"domino_jc_project/pkg/telemetry"
	"domino_jc_project/pkg/ws"
)

func main() {
	logger := telemetry.InitLogger()
	logger.Info("initializing Domino JC game server")

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":2112", nil); err != nil {
			telemetry.AppLogger.Error("Telemetry server failed", "error", err)
		}
	}()

	// 1. Grab connection target from environment or fall back to local default
	dgraphAddr := os.Getenv("DGRAPH_ALPHA_GRPC")
	if dgraphAddr == "" {
		dgraphAddr = "localhost:9080" // Dgraph Alpha gRPC default port
	}

	dbConfig := database.Config{
		Address: dgraphAddr,
	}

	// 2. Initialize the gRPC Connection Pool
	dgClient, grpcConn, err := database.InitDgraphClient(dbConfig)
	if err != nil {
		logger.Error("failed to initialize Dgraph connection pool", slog.Any("error", err))
		os.Exit(1)
	}

	// 3. Defer closing the connection pool to handle graceful application shutdown
	defer func() {
		logger.Info("shutting down game server: closing Dgraph gRPC connection pool")
		if err := grpcConn.Close(); err != nil {
			logger.Warn("error closing gRPC connection pool", slog.Any("error", err))
		} else {
			logger.Info("gRPC connection pool successfully disconnected")
		}
	}()

	// 4. Instantiate the state persistence repository layer
	gameRepo := repository.NewDgraphGameRepository(dgClient)

	// 5. Wire the game orchestrator with repository-backed persistence
	gameManager := engine.NewGameManager(gameRepo)

	// 6. Register ACTIVE session IDs from Dgraph for lazy recovery after restart
	if err := gameManager.BootstrapActiveSessions(context.Background(), gameRepo); err != nil {
		logger.Error("failed to bootstrap active sessions for crash recovery", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("game repository layer and orchestrator initialized")

	// 7. Start the WebSocket hub with bidirectional event routing into GameManager.
	ledgerBreaker := newBreakerWithMetrics("ledger")
	ratingBreaker := newBreakerWithMetrics("rating")

	hub := ws.NewHub(gameManager)
	ratingWorker := engine.NewRatingWorker(
		gameRepo,
		engine.WithStatsBroadcaster(hub),
		engine.WithRatingBreaker(ratingBreaker),
	)
	ledgerWorker := engine.NewLedgerWorker(
		gameRepo,
		0,
		engine.WithRatingProcessor(ratingWorker),
		engine.WithLedgerBreaker(ledgerBreaker),
	)
	hub.SetMatchLedger(ledgerWorker)
	gameManager.SetMatchTerminator(hub)
	go hub.Run()
	go ledgerWorker.Run()

	wsHandler := ws.NewHandler(hub)
	statsHandler := api.NewStatsHandler(gameRepo)

	limiter := telemetry.NewTokenBucketLimiter(10.0, 20.0, 1*time.Hour)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/ws/connect", limiter.RateLimitMiddleware(http.HandlerFunc(wsHandler.ServeConnect)))

	apiMux := http.NewServeMux()
	statsHandler.Register(apiMux)
	mux.Handle("/api/", limiter.RateLimitMiddleware(apiMux))

	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8380"
	}

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           telemetry.TraceMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("HTTP server listening",
			slog.String("addr", httpAddr),
			slog.String("ws_endpoint", httpAddr+"/ws/connect"),
			slog.String("metrics_endpoint", httpAddr+"/metrics"),
		)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	// 8. Keep the server alive and listen for termination signals
	logger.Info("Domino JC game server is operational")

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	sig := <-shutdownChan
	logger.Info("captured shutdown signal, initiating teardown", slog.String("signal", sig.String()))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("HTTP server shutdown error", slog.Any("error", err))
	}
}

func newBreakerWithMetrics(name string) *resilience.Breaker {
	cfg := resilience.DefaultBreakerConfig(name)
	cfg.OnStateChange = func(workerName string, _, to gobreaker.State) {
		telemetry.CircuitBreakerState.WithLabelValues(workerName).Set(gobreakerStateToMetric(to))
	}
	breaker := resilience.NewBreaker(cfg)
	telemetry.CircuitBreakerState.WithLabelValues(name).Set(resilienceStateToMetric(breaker.State()))
	return breaker
}

func resilienceStateToMetric(s resilience.State) float64 {
	switch s {
	case resilience.StateClosed:
		return 0
	case resilience.StateHalfOpen:
		return 1
	case resilience.StateOpen:
		return 2
	default:
		return -1
	}
}

func gobreakerStateToMetric(s gobreaker.State) float64 {
	switch s {
	case gobreaker.StateClosed:
		return 0
	case gobreaker.StateHalfOpen:
		return 1
	case gobreaker.StateOpen:
		return 2
	default:
		return -1
	}
}
