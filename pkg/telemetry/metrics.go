package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const Namespace = "domino_jc"

var (
	// ActiveWSConnections tracks live connected players
	ActiveWSConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "websocket",
		Name:      "active_connections",
		Help:      "The total number of active player WebSocket connections.",
	})

	// WSTxMessageLatency tracks broadcast speeds across client channels
	WSTxMessageLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "websocket",
		Name:      "broadcast_duration_seconds",
		Help:      "Latency of WebSocket outbound message delivery.",
		Buckets:   []float64{.001, .005, .01, .05, .1, .5, 1},
	}, []string{"message_type"})

	// LedgerCommitDuration tracks transaction speeds against Dgraph
	LedgerCommitDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "ledger",
		Name:      "commit_duration_seconds",
		Help:      "Time taken to commit match immutable states to Dgraph.",
		Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"status"})

	// CircuitBreakerState exports state values: 0=Closed, 1=Half-Open, 2=Open
	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "resilience",
		Name:      "circuit_breaker_state",
		Help:      "Current operating state of the Dgraph engine circuit breaker (0=Closed, 1=Half-Open, 2=Open).",
	}, []string{"worker_name"})
)
