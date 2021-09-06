package main

import "github.com/prometheus/client_golang/prometheus"

var (
	AuditConnectionEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "audit",
			Name:      "audit_connection_event",
			Help:      "Counter of audit connection event",
		}, []string{"event"})

	AuditStmtEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "audit",
			Name:      "audit_stmt_event",
			Help:      "Counter of audit stmt event",
		}, []string{"sql_type"})

	AuditCommandEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "audit",
			Name:      "audit_command_event",
			Help:      "Counter of audit command event",
		}, []string{"sql_type"})
)

func initMetric() error {
	prometheus.Register(AuditConnectionEventCounter)
	prometheus.Register(AuditStmtEventCounter)
	prometheus.Register(AuditCommandEventCounter)
	return nil
}
