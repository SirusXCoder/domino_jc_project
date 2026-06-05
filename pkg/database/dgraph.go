package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/dgraph-io/dgo/v210"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultGRPCPort = "9080"

// DgraphClient wraps the official Dgraph Go client and manages underlying gRPC connections.
type DgraphClient struct {
	client *dgo.Dgraph
	conns  []*grpc.ClientConn
}

// NewClient establishes a gRPC connection pool to one or more Dgraph Alpha nodes.
// Each host may be a bare hostname/IP (port 9080 is assumed) or a full host:port address.
func NewClient(hosts []string) (*DgraphClient, error) {
	if len(hosts) == 0 {
		return nil, fmt.Errorf("at least one Dgraph Alpha host is required")
	}

	apiClients := make([]api.DgraphClient, 0, len(hosts))
	conns := make([]*grpc.ClientConn, 0, len(hosts))

	for _, host := range hosts {
		addr := normalizeHost(host)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			closeConns(conns)
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
		conns = append(conns, conn)
		apiClients = append(apiClients, api.NewDgraphClient(conn))
	}

	return &DgraphClient{
		client: dgo.NewDgraphClient(apiClients...),
		conns:  conns,
	}, nil
}

// Close tears down all gRPC connections held by the client.
func (c *DgraphClient) Close() error {
	return closeConns(c.conns)
}

// ExecuteQuery runs a DQL query inside a read-write transaction.
func (c *DgraphClient) ExecuteQuery(ctx context.Context, query string, vars map[string]string) (*api.Response, error) {
	txn := c.client.NewTxn()
	defer txn.Discard(ctx)

	return txn.QueryWithVars(ctx, query, vars)
}

// ExecuteMutation applies a mutation and commits the transaction.
func (c *DgraphClient) ExecuteMutation(ctx context.Context, mu *api.Mutation) (*api.Response, error) {
	txn := c.client.NewTxn()
	defer txn.Discard(ctx)

	resp, err := txn.Mutate(ctx, mu)
	if err != nil {
		return nil, err
	}

	if err := txn.Commit(ctx); err != nil {
		return nil, err
	}

	return resp, nil
}

func normalizeHost(host string) string {
	if strings.Contains(host, ":") {
		return host
	}
	return host + ":" + defaultGRPCPort
}

func closeConns(conns []*grpc.ClientConn) error {
	var firstErr error
	for _, conn := range conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
