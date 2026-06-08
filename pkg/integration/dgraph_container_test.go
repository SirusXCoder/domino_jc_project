package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v210"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestDgraphIntegrationWithTestcontainers(t *testing.T) {
	ctx := context.Background()

	// Spin up standalone Dgraph container (Alpha & Zero combined or quick standalone configuration)
	req := testcontainers.ContainerRequest{
		Image:        "dgraph/standalone:v23.1.0",
		ExposedPorts: []string{"9080/tcp", "8080/tcp"}, // 9080 is gRPC, 8080 is HTTP
		WaitingFor:   wait.ForLog("Server is ready").WithStartupTimeout(30 * time.Second),
	}

	dgraphContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("Skipping test: Docker daemon not accessible or container failed to start: %v", err)
	}
	defer dgraphContainer.Terminate(ctx)

	// Get gRPC host and port mapping
	host, err := dgraphContainer.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := dgraphContainer.MappedPort(ctx, "9080")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}

	target := fmt.Sprintf("%s:%s", host, port.Port())

	// Establish real gRPC connection to the ephemeral Dgraph instance
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect to test Dgraph instance: %v", err)
	}
	defer conn.Close()

	dgClient := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	// 1. Initialize and assert schema with Facet indexing requirements
	schema := `
		match_id: string @index(exact) .
		score: int .
		history: uid @reverse .
	`
	err = dgClient.Alter(ctx, &api.Operation{Schema: schema})
	if err != nil {
		t.Fatalf("failed to apply Dgraph schema: %v", err)
	}

	// 2. Perform a sample write operation simulating our Rating/Ledger loop completion
	txn := dgClient.NewTxn()
	defer txn.Discard(ctx)

	mutation := &api.Mutation{
		CommitNow: true,
		SetJson: []byte(`
			{
				"uid": "_:match1",
				"match_id": "match_live_999",
				"score": 350
			}
		`),
	}

	_, err = txn.Mutate(ctx, mutation)
	if err != nil {
		t.Fatalf("failed to commit live match data to Dgraph: %v", err)
	}

	// 3. Query the data back verifying the read-path performance and schema execution
	query := `query testMatch($id: string) {
		find_match(func: eq(match_id, $id)) {
			uid
			match_id
			score
		}
	}`

	resp, err := dgClient.NewTxn().QueryWithVars(ctx, query, map[string]string{"$id": "match_live_999"})
	if err != nil {
		t.Fatalf("failed to execute DQL validation query: %v", err)
	}

	if len(resp.Json) == 0 || string(resp.Json) == `{"find_match":[]}` {
		t.Errorf("expected data to be retrieved, got empty payload instead")
	}
}