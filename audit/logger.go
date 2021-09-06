package main

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"go.uber.org/zap"
)

var logger *zap.Logger

func initLog() (err error) {
	var filename = "tidb-audit.log" // TODO: Tweak log configuration by TiDB start arguments.
	tidbConf := config.GetGlobalConfig()
	baseFolder := filepath.Dir(tidbConf.Log.SlowQueryFile)
	conf := &log.Config{
		Level:  "info",
		Format: "text",
		File: log.FileLogConfig{
			MaxSize:  tidbConf.Log.File.MaxSize,
			Filename: filepath.Join(baseFolder, filename),
		},
	}
	logger, _, err = log.InitLogger(conf)
	if err != nil {
		return
	}
	return
}

type logRecord struct {
	columns []zap.Field
}

type logTrait interface {
	addConn(connInfo *variable.ConnectionInfo, reason string) logTrait
	addGeneral(info *variable.ConnectionInfo, names map[string]struct{}, names2 []string, normalized string, sctx *variable.SessionVars, cmd string) logTrait
	log()
}

func auditLog(ctx context.Context, eventClass, eventSubClass string, statusCode int) logTrait {
	now, id := auditID()
	l := &logRecord{}
	costTime := time.Duration(0)
	if start := ctx.Value("ExecStartTime"); start != nil {
		costTime = time.Since(start.(time.Time))
	}
	l.columns = append(l.columns, []zap.Field{
		zap.String("ID", id),
		zap.Time("TIMESTAMP", now),
		zap.String("EVENT_CLASS", eventClass),
		zap.String("EVENT_SUBCLASS", eventSubClass),
		zap.Int("STATUS_CODE", statusCode),
		zap.Float64("COST_TIME", float64(costTime)/float64(time.Microsecond)),
	}...)
	return l
}

func (l *logRecord) addConn(connInfo *variable.ConnectionInfo, reason string) logTrait {
	l.fields([]zap.Field{
		zap.String("HOST", connInfo.Host),
		zap.String("CLIENT_IP", connInfo.ClientIP),
		zap.String("USER", connInfo.User),
		zap.Strings("DATABASES", []string{connInfo.DB}),
		zap.Strings("TABLES", []string{}),
		zap.String("SQL_TEXT", ""),
		zap.Uint64("ROWS", 0),
		zap.String("CLIENT_PORT", connInfo.ClientPort),
		zap.Uint32("CONNECTION_ID", connInfo.ConnectionID),
		zap.String("CONNECTION_TYPE", connInfo.ConnectionType),
		zap.Int("SERVER_ID", connInfo.ServerID),
		zap.Int("SERVER_PORT", connInfo.ServerPort),
		zap.Float64("DURATION", connInfo.Duration),
		zap.String("SERVER_OS_LOGIN_USER", connInfo.ServerOSLoginUser),
		zap.String("OS_VERSION", connInfo.OSVersion),
		zap.String("CLIENT_VERSION", connInfo.ClientVersion),
		zap.String("SERVER_VERSION", connInfo.ServerVersion),
		zap.String("AUDIT_VERSION", ""),
		zap.String("SSL_VERSION", connInfo.SSLVersion),
		zap.Int("PID", connInfo.PID),
		zap.String("Reason", reason),
	})
	return l
}

func (l *logRecord) addGeneral(connInfo *variable.ConnectionInfo, dbNames map[string]struct{}, tableNames []string, normalized string, sctx *variable.SessionVars, cmd string) logTrait {
	l.fields([]zap.Field{
		zap.String("HOST", connInfo.Host),
		zap.String("CLIENT_IP", connInfo.ClientIP),
		zap.String("USER", connInfo.User),
		zap.Strings("DATABASES", setToSlice(dbNames)),
		zap.Strings("TABLES", tableNames),
		zap.String("SQL_TEXT", normalized),
		zap.Uint64("ROWS", sctx.StmtCtx.AffectedRows()),
		zap.Uint32("CONNECTION_ID", connInfo.ConnectionID),
		zap.String("CLIENT_PORT", connInfo.ClientPort),
		zap.Int("PID", connInfo.PID),
		zap.String("COMMAND", cmd),
		zap.String("SQL_STATEMENTS", sctx.StmtCtx.StmtType),
	})
	return l
}

func setToSlice(set map[string]struct{}) (slice []string) {
	slice = make([]string, 0, len(set))
	for val := range set {
		slice = append(slice, val)
	}
	return
}

func (l *logRecord) field(f zap.Field) *logRecord {
	l.columns = append(l.columns, f)
	return l
}

func (l *logRecord) fields(fs []zap.Field) logTrait {
	l.columns = append(l.columns, fs...)
	return l
}

func (l *logRecord) log() {
	logger.Info("", l.columns...)
}

var last struct {
	sync.Mutex
	ts  int64
	seq int
}

func auditID() (t time.Time, id string) {
	last.Lock()
	var seq int
	t = time.Now()
	newTs := t.UnixNano() / int64(time.Second)
	timeMoveBack := false
	if newTs > last.ts {
		seq = 0
		last.seq = 0
		last.ts = newTs
	} else {
		if newTs < last.ts {
			timeMoveBack = true
		}
		seq = last.seq
		last.seq++ // overflow be negative is ok for audit ID.
	}
	last.Unlock()
	if timeMoveBack {
		log.Warn("time is moving backwards")
	}
	id = strconv.FormatInt(newTs, 10) + strconv.Itoa(seq)
	return
}
