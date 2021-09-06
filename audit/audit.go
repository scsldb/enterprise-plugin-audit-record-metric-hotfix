package main

import (
	"context"

	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// OnInit implements TiDB plugin's OnInit SPI.
func OnInit(ctx context.Context, manifest *plugin.Manifest) error {
	err := global.init()
	if err != nil {
		return err
	}
	err = initLog()
	if err != nil {
		return err
	}
	err = initMetric()
	if err != nil {
		return err
	}
	return err
}

// OnFlush implements TiDB plugin's OnShutdown SPI.
func OnFlush(ctx context.Context, manifest *plugin.Manifest) error {
	return global.update()
}

// OnConnectionEvent implements TiDB Audit plugin's OnConnectionEvent SPI.
func OnConnectionEvent(ctx context.Context, event plugin.ConnectionEvent, connInfo *variable.ConnectionInfo) error {
	if event == plugin.PreAuth {
		return nil
	}
	var reason string
	if r := ctx.Value(plugin.RejectReasonCtxValue{}); r != nil {
		reason = r.(string)
	}
	auditLog(ctx, "CONNECTION", event.String(), 0).addConn(connInfo, reason).log()
	auditTable(ctx, "CONNECTION", event.String(), 0).addConn(connInfo, reason).log()
	AuditConnectionEventCounter.WithLabelValues(event.String()).Inc()
	return nil
}

// OnGeneralEvent implements TiDB Audit plugin's OnGeneralEvent SPI.
func OnGeneralEvent(ctx context.Context, sctx *variable.SessionVars, event plugin.GeneralEvent, cmd string) {
	connInfo := sctx.ConnectionInfo
	if connInfo == nil {
		return
	}
	var (
		tableNames []string
		dbNames    = make(map[string]struct{})
	)
	eventClass, eventSubClass := "GENERAL", ""
	if len(sctx.StmtCtx.Tables) > 0 {
		eventClass = "TABLE_ACCESS"
		eventSubClass = sctx.StmtCtx.StmtType
		for _, tbl := range sctx.StmtCtx.Tables {
			dbNames[tbl.DB] = struct{}{}
			tableNames = append(tableNames, tbl.Table)
			if !global.get().check(connInfo.User, tbl.DB, tbl.Table, eventSubClass) {
				return
			}
		}
	}
	normalized, _ := sctx.StmtCtx.SQLDigest()
	auditLog(ctx, eventClass, eventSubClass, 0).addGeneral(connInfo, dbNames, tableNames, normalized, sctx, cmd).log()
	auditTable(ctx, eventClass, eventSubClass, 0).addGeneral(connInfo, dbNames, tableNames, normalized, sctx, cmd).log()
	AuditStmtEventCounter.WithLabelValues(sctx.StmtCtx.StmtType).Inc()
	AuditCommandEventCounter.WithLabelValues(cmd).Inc()
	return
}
