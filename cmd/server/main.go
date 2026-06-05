package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"domino_jc_project/pkg/database"
	"domino_jc_project/pkg/engine"
	"domino_jc_project/pkg/repository"
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
	_ = gameManager
	log.Println("Game repository layer and orchestrator successfully initialized with live connection pool.")

	// 6. Keep the server alive and listen for termination signals
	log.Println("Domino JC Game Server is fully operational and running...")
	
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	// Block until a termination signal is caught
	sig := <-shutdownChan
	log.Printf("Captured system signal (%v). Initiating safe teardown sequence...", sig)
}