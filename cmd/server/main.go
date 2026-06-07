package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"domino_jc_project/pkg/api"
	"domino_jc_project/pkg/database"
	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/repository"
	"domino_jc_project/pkg/ws"
)

func main() {
	log.Println("=== Initializing Domino JC Game Server ===")

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
		log.Fatalf("CRITICAL: Failed to initialize Dgraph connection pool: %v", err)
	}

	// 3. Defer closing the connection pool to handle graceful application shutdown
	defer func() {
		log.Println("Shutting down game server: Closing Dgraph gRPC connection pool...")
		if err := grpcConn.Close(); err != nil {
			log.Printf("Warning: Error closing gRPC connection pool: %v", err)
		} else {
			log.Println("gRPC connection pool successfully disconnected.")
		}
	}()

	// 4. Instantiate the state persistence repository layer
	gameRepo := repository.NewDgraphGameRepository(dgClient)

	// 5. Wire the game orchestrator with repository-backed persistence
	gameManager := engine.NewGameManager(gameRepo)

	// 6. Register ACTIVE session IDs from Dgraph for lazy recovery after restart
	if err := gameManager.BootstrapActiveSessions(context.Background(), gameRepo); err != nil {
		log.Fatalf("CRITICAL: Failed to bootstrap active sessions for crash recovery: %v", err)
	}
	log.Println("Game repository layer and orchestrator successfully initialized with live connection pool.")

	// 7. Start the WebSocket hub with bidirectional event routing into GameManager.
	ratingWorker := engine.NewRatingWorker(gameRepo)
	ledgerWorker := engine.NewLedgerWorker(gameRepo, 0, engine.WithRatingProcessor(ratingWorker))
	go ledgerWorker.Run()

	hub := ws.NewHub(gameManager, ws.WithMatchLedger(ledgerWorker))
	gameManager.SetMatchTerminator(hub)
	go hub.Run()

	wsHandler := ws.NewHandler(hub)
	statsHandler := api.NewStatsHandler(gameRepo)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws/connect", wsHandler.ServeConnect)
	statsHandler.Register(mux)

	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8380"
	}

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("WebSocket endpoint listening on %s/ws/connect", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("CRITICAL: HTTP server failed: %v", err)
		}
	}()

	// 8. Keep the server alive and listen for termination signals
	log.Println("Domino JC Game Server is fully operational and running...")

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	sig := <-shutdownChan
	log.Printf("Captured system signal (%v). Initiating safe teardown sequence...", sig)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Warning: HTTP server shutdown error: %v", err)
	}
}