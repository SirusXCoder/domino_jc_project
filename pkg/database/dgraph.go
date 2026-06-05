package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/dgo/v210"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config holds the configuration details for connecting to Dgraph.
type Config struct {
	Address string // e.g., "localhost:9080"
}

// InitDgraphClient establishes a gRPC connection pool and returns an initialized Dgraph client.
// It also returns the underlying grpc.ClientConn so it can be closed gracefully upon application shutdown.
func InitDgraphClient(cfg Config) (*dgo.Dgraph, *grpc.ClientConn, error) {
	// Set up a connection timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Connecting to Dgraph alpha node at %s...", cfg.Address)

	// Dial the Dgraph gRPC endpoint. Using insecure credentials for local setup; 
	// exchange for transport credentials (TLS) in production if needed.
	conn, err := grpc.DialContext(ctx, cfg.Address, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // Block until the connection is established or times out
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to Dgraph via gRPC: %w", err)
	}

	// Create the Dgraph client using the operational gRPC connection pool
	dgraphClient := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	log.Println("Successfully initialized Dgraph client connection pool.")
	return dgraphClient, conn, nil
}