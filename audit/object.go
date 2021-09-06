package main

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	createTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_audit_table_access (
  id int not null auto_increment, 
  user varchar(32) NOT NULL comment "username need be audit(regex)",
  db varchar(32) NOT NULL comment "db name need be audit(regex)",
  tbl varchar(32) NOT NULL comment "table name need be audit(regex)",
  access_type varchar(1024) NOT NULL comment "table access types split by comma",
  create_time timestamp NULL DEFAULT NULL comment "record create time",
  update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment "record last update time",
  UNIQUE KEY(user, db, tbl),
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`
	selectTable = `select user, db, tbl, access_type from mysql.tidb_audit_table_access`
	createLog   = `create table if not exists mysql.tidb_audit_log (
  id varchar(500),
  user varchar(1000),
  time timestamp,
  event varchar(120),
  sub_event varchar(255),
  status_code int,
  cost_time float,
  host varchar(500),
  client_ip varchar(255),
  dbs varchar(1000),
  tables varchar(5000),
  statement text,
  affect_rows int,
  client_port varchar(255),
  connection_id int,
  connection_type varchar(255),
  server_id int,
  server_port int,
  server_os_login_user varchar(1000),
  server_os_version varchar(1000),
  client_version varchar(1000),
  server_version varchar(1000),
  pid int,
  reason text
)`
	createLogView = `create view mysql.tidb_audit_current_user as select * from mysql.tidb_audit_log where user = substr(current_user(), 1, LOCATE('@', current_user()) - 1)`
)

var global = &handle{}

type handle struct {
	mu struct {
		sync.RWMutex
		dom *domain.Domain
	}
	value atomic.Value
}

func (h *handle) init() error {
	h.mu.RLock()
	dom := h.mu.dom
	h.mu.RUnlock()
	if dom != nil {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	dom, err := session.GetDomain(nil)
	if err != nil {
		return err
	}
	h.mu.dom = dom

	pool := h.mu.dom.SysSessionPool()
	sctx, err := pool.Get()
	if err != nil {
		return err
	}
	defer pool.Put(sctx)

	ctx := context.Background()
	rss, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, createTable)
	if err != nil {
		return err
	}
	for _, rs := range rss {
		_ = rs.Close()
	}
	rss, err = sctx.(sqlexec.SQLExecutor).Execute(ctx, createLog)
	if err != nil {
		return err
	}
	for _, rs := range rss {
		_ = rs.Close()
	}
	rss, err = sctx.(sqlexec.SQLExecutor).Execute(ctx, createLogView)
	if err == nil {
		for _, rs := range rss {
			_ = rs.Close()
		}
	}
	return h.update()
}

func (h *handle) getDomain() *domain.Domain {
	return h.mu.dom
}

func (h *handle) update() error {
	pool := h.mu.dom.SysSessionPool()
	tmp, err := pool.Get()
	if err != nil {
		return err
	}

	defer pool.Put(tmp)
	sctx := tmp.(sessionctx.Context)

	var audits AuditObjects
	err = audits.loadFromTable(sctx)
	if err != nil {
		return err
	}

	h.value.Store(&audits)
	return nil
}

func (h *handle) get() *AuditObjects {
	return h.value.Load().(*AuditObjects)
}

type AuditObjects []AuditItem

type AuditItem struct {
	AccessType map[string]bool
	Table      *regexp.Regexp
	User       *regexp.Regexp
	DB         *regexp.Regexp
}

func (a *AuditObjects) check(user, db, tbl, subClass string) bool {
	for _, item := range *a {
		if (item.Table == nil || item.Table.MatchString(tbl)) &&
			(item.DB == nil || item.DB.MatchString(db)) &&
			(item.User == nil || item.User.MatchString(user)) &&
			(len(item.AccessType) == 0 || item.AccessType[subClass]) {
			return true
		}
	}
	return false
}

func (a *AuditObjects) loadFromTable(sctx sessionctx.Context) error {
	ctx := context.Background()
	tmp, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, selectTable)
	if err != nil {
		return err
	}
	rs := tmp[0]
	defer rs.Close()

	chk := rs.NewChunk()
	for {
		err = rs.Next(context.TODO(), chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			return nil
		}
		it := chunk.NewIterator4Chunk(chk)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			err = a.decodeTableRow(row)
			if err != nil {
				log.Error("", zap.Error(err))
				// Ignore this row and continue...
				continue
			}
		}
		// NOTE: decodeTableRow decodes data from a chunk Row, that is a shallow copy.
		// The result will reference memory in the chunk, so the chunk must not be reused
		// here, otherwise some werid bug will happen!
		chk = chunk.Renew(chk, sctx.GetSessionVars().MaxChunkSize)
	}
}

func (a *AuditObjects) decodeTableRow(row chunk.Row) error {
	user := row.GetString(0)
	db := row.GetString(1)
	tbl := row.GetString(2)
	accessType := row.GetString(3)
	var (
		userRegex, tblRegex, dbRegex *regexp.Regexp
		err                          error
	)
	if len(user) > 0 {
		userRegex, err = regexp.Compile(user)
		if err != nil {
			return err
		}
	}
	if len(tbl) > 0 {
		tblRegex, err = regexp.Compile(tbl)
		if err != nil {
			return err
		}
	}
	if len(db) > 0 {
		dbRegex, err = regexp.Compile(db)
		if err != nil {
			return err
		}
	}
	accessMap := make(map[string]bool)
	if len(accessType) > 0 {
		for _, item := range strings.Split(accessType, ",") {
			accessMap[item] = true
		}
	}
	item := AuditItem{
		User:       userRegex,
		DB:         dbRegex,
		Table:      tblRegex,
		AccessType: accessMap,
	}
	*a = append(*a, item)
	return nil
}
