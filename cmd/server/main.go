package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sony/gobreaker"

	"domino_jc_project/pkg/api"
	"domino_jc_project/pkg/broker"
	"domino_jc_project/pkg/consensus"
	"domino_jc_project/pkg/database"
	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/game"
	"domino_jc_project/pkg/gateway"
	"domino_jc_project/pkg/repository"
	"domino_jc_project/pkg/resilience"
	"domino_jc_project/pkg/telemetry"
	"domino_jc_project/pkg/ws"
)

func main() {
	nodeID := flag.String("id", "", "unique node identifier (e.g. node-1)")
	raftAddr := flag.String("raft-addr", "", "Raft RPC listen address (e.g. :50051)")
	gatewayAddr := flag.String("gateway-addr", "", "HTTP/WebSocket gateway listen address (e.g. :8080)")
	peersFlag := flag.String("peers", "", "comma-separated peer list: id=addr,id=addr")
	flag.Parse()

	if err := validateNodeFlags(*nodeID, *raftAddr, *gatewayAddr); err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(1)
	}

	logger := telemetry.InitLogger()
	logger.Info("initializing Domino JC game server",
		slog.String("node_id", *nodeID),
		slog.String("raft_addr", *raftAddr),
		slog.String("gateway_addr", *gatewayAddr),
	)

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":2112", nil); err != nil {
			telemetry.AppLogger.Error("Telemetry server failed", "error", err)
		}
	}()

	peerAddrs, err := parsePeerList(*peersFlag)
	if err != nil {
		logger.Error("failed to parse --peers", slog.Any("error", err))
		os.Exit(1)
	}
	clusterPeers := buildClusterPeers(*nodeID, *raftAddr, peerAddrs)
	logger.Info("Raft cluster peers configured", slog.Int("peer_count", len(clusterPeers)))

	dgraphAddr := os.Getenv("DGRAPH_ALPHA_GRPC")
	if dgraphAddr == "" {
		dgraphAddr = "localhost:9080"
	}

	dbConfig := database.Config{Address: dgraphAddr}
	dgClient, grpcConn, err := database.InitDgraphClient(dbConfig)
	if err != nil {
		logger.Error("failed to initialize Dgraph connection pool", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() {
		logger.Info("shutting down game server: closing Dgraph gRPC connection pool")
		if err := grpcConn.Close(); err != nil {
			logger.Warn("error closing gRPC connection pool", slog.Any("error", err))
		} else {
			logger.Info("gRPC connection pool successfully disconnected")
		}
	}()

	gameRepo := repository.NewDgraphGameRepository(dgClient)
	gameManager := engine.NewGameManager(gameRepo)

	if err := gameManager.BootstrapActiveSessions(context.Background(), gameRepo); err != nil {
		logger.Error("failed to bootstrap active sessions for crash recovery", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("game repository layer and orchestrator initialized")

	raftCtx, raftCancel := context.WithCancel(context.Background())
	defer raftCancel()

	matchFSM := consensus.NewManagedGameFSM(raftCtx, gameManager)

	var raftStorage *database.RaftStorage
	if dataDir := strings.TrimSpace(os.Getenv("RAFT_DATA_DIR")); dataDir != "" {
		raftStorage, err = database.NewRaftStorage(dataDir)
		if err != nil {
			logger.Error("failed to initialize Raft storage", slog.String("data_dir", dataDir), slog.Any("error", err))
			os.Exit(1)
		}
		logger.Info("Raft durable storage enabled", slog.String("data_dir", dataDir))
	}

	var raftNode *consensus.RaftNode
	if raftStorage != nil {
		raftNode, err = consensus.NewRaftNodeWithStorage(*nodeID, clusterPeers, matchFSM, raftStorage)
	} else {
		raftNode = consensus.NewRaftNode(*nodeID, clusterPeers, matchFSM)
	}
	if err != nil {
		logger.Error("failed to initialize Raft node", slog.Any("error", err))
		os.Exit(1)
	}

	if compactThreshold, ok := compactThresholdFromEnv(); ok {
		raftNode.SetCompactThreshold(compactThreshold)
		logger.Info("Raft log compaction threshold configured",
			slog.Uint64("compact_threshold", compactThreshold),
			slog.String("env", "RAFT_COMPACT_THRESHOLD"),
		)
	}

	raftTransport := consensus.NewNetworkTransport(raftCtx, raftNode)
	if err := raftTransport.StartServer(*raftAddr); err != nil {
		logger.Error("failed to start Raft transport", slog.String("addr", *raftAddr), slog.Any("error", err))
		os.Exit(1)
	}
	raftNode.Start(raftCtx)
	logger.Info("Starting Raft node", slog.String("addr", *raftAddr), slog.String("node_id", *nodeID))

	replicatedHandler := gateway.NewReplicatedGameHandler(raftNode, gameManager)
	hub := ws.NewHub(replicatedHandler)
	gateway.WireSessionReplication(raftNode, hub)

	eventBroker := broker.NewInMemoryBroker()
	defer eventBroker.Close()

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	ledgerBreaker := newBreakerWithMetrics("ledger")
	ratingBreaker := newBreakerWithMetrics("rating")

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
	go ledgerWorker.Run()

	go func() {
		if err := game.StartBackgroundWorkersWithConfig(workerCtx, eventBroker, game.WorkerConfig{
			OnMatchEnded: func(ctx context.Context, payload game.MatchEndedPayload) {
				if payload.Outcome == nil || payload.Session == nil {
					logger.Warn("match.ended event missing outcome or session",
						slog.String("session_id", payload.SessionID),
					)
					return
				}
				hub.TerminateMatch(ctx, payload.SessionID, payload.Outcome, payload.Session)
			},
		}); err != nil {
			logger.Error("failed to start background event workers", slog.Any("error", err))
		}
	}()

	gameEngine := game.NewGameEngine(eventBroker)
	gameManager.SetGameEngine(gameEngine)

	go hub.Run()

	wsHandler := ws.NewHandler(hub)
	statsHandler := api.NewStatsHandler(gameRepo)

	httpPeerAddrs, err := buildHTTPPeerAddresses(*nodeID, *gatewayAddr, os.Getenv("GATEWAY_HTTP_PEERS"))
	if err != nil {
		logger.Error("failed to parse GATEWAY_HTTP_PEERS", slog.Any("error", err))
		os.Exit(1)
	}

	var gatewayOpts []gateway.MatchGatewayOption
	if len(httpPeerAddrs) > 0 {
		gatewayOpts = append(gatewayOpts, gateway.WithHTTPPeerAddresses(httpPeerAddrs))
	}

	matchGateway := gateway.NewMatchGateway(raftNode, gameManager, gatewayOpts...)
	gatewayHandler := api.NewGatewayHandler(raftNode)

	limiter := telemetry.NewTokenBucketLimiter(10.0, 20.0, 1*time.Hour)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/ws/connect", limiter.RateLimitMiddleware(http.HandlerFunc(wsHandler.ServeConnect)))
	matchGateway.Register(mux)

	apiMux := http.NewServeMux()
	statsHandler.Register(apiMux)
	gatewayHandler.Register(apiMux)
	mux.Handle("/api/", limiter.RateLimitMiddleware(apiMux))

	httpServer := &http.Server{
		Addr:              *gatewayAddr,
		Handler:           telemetry.TraceMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("Gateway listening",
			slog.String("addr", *gatewayAddr),
			slog.String("ws_endpoint", *gatewayAddr+"/ws/connect"),
			slog.String("match_read", *gatewayAddr+"/match/read"),
			slog.String("match_create", *gatewayAddr+"/match/create"),
			slog.String("match_mutate", *gatewayAddr+"/match/mutate"),
			slog.String("debug_fill_log", *gatewayAddr+"/debug/fill-log"),
			slog.String("metrics_endpoint", *gatewayAddr+"/metrics"),
		)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP gateway failed", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	logger.Info("Domino JC game server is operational", slog.String("node_id", *nodeID))

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	sig := <-shutdownChan
	logger.Info("captured shutdown signal, initiating teardown",
		slog.String("signal", sig.String()),
		slog.String("node_id", *nodeID),
	)

	workerCancel()
	ledgerWorker.Stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("HTTP gateway shutdown error", slog.Any("error", err))
	} else {
		logger.Info("HTTP gateway stopped")
	}

	raftCancel()
	raftTransport.Shutdown()
	raftTransport.Wait()
	logger.Info("Raft transport stopped")

	if err := raftNode.FlushStorage(); err != nil {
		logger.Warn("failed to flush Raft storage", slog.Any("error", err))
	} else if raftStorage != nil {
		logger.Info("Raft state flushed to disk", slog.String("data_dir", raftStorage.DataDir()))
	}

	logger.Info("Domino JC game server shutdown complete", slog.String("node_id", *nodeID))
}

func compactThresholdFromEnv() (uint64, bool) {
	raw := strings.TrimSpace(os.Getenv("RAFT_COMPACT_THRESHOLD"))
	if raw == "" {
		return 0, false
	}
	threshold, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || threshold == 0 {
		return 0, false
	}
	return threshold, true
}

func validateNodeFlags(nodeID, raftAddr, gatewayAddr string) error {
	if strings.TrimSpace(nodeID) == "" {
		return fmt.Errorf("--id is required (e.g. --id=node-1)")
	}
	if strings.TrimSpace(raftAddr) == "" {
		return fmt.Errorf("--raft-addr is required (e.g. --raft-addr=:50051)")
	}
	if strings.TrimSpace(gatewayAddr) == "" {
		return fmt.Errorf("--gateway-addr is required (e.g. --gateway-addr=:8080)")
	}
	return nil
}

func parsePeerList(raw string) (map[string]string, error) {
	peers := make(map[string]string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return peers, nil
	}

	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid peer entry %q: expected id=address", entry)
		}
		id := strings.TrimSpace(parts[0])
		addr := strings.TrimSpace(parts[1])
		if id == "" || addr == "" {
			return nil, fmt.Errorf("invalid peer entry %q: id and address are required", entry)
		}
		peers[id] = addr
	}
	return peers, nil
}

func buildClusterPeers(nodeID, raftAddr string, remotePeers map[string]string) map[string]string {
	cluster := make(map[string]string, len(remotePeers)+1)
	for id, addr := range remotePeers {
		cluster[id] = addr
	}
	cluster[nodeID] = raftAddr
	return cluster
}

func buildHTTPPeerAddresses(nodeID, gatewayAddr, raw string) (map[string]string, error) {
	peers, err := parsePeerList(raw)
	if err != nil {
		return nil, err
	}
	normalized := make(map[string]string, len(peers)+1)
	for id, addr := range peers {
		normalized[id] = gatewayHTTPBase(addr)
	}
	normalized[nodeID] = gatewayHTTPBase(gatewayAddr)
	return normalized, nil
}

func gatewayHTTPBase(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	host := addr
	if strings.HasPrefix(addr, ":") {
		host = "localhost" + addr
	}
	return "http://" + host
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
