package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	createTableSQL = `create table if not exists mysql.whitelist (
		id int not null auto_increment primary key,
		name varchar(16) unique,
		list TEXT
	)`
	selectTableSQL = `select * from mysql.whitelist`
)

// OnInit implements TiDB plugin's OnInit SPI.
func OnInit(ctx context.Context, manifest *plugin.Manifest) error {
	global.init()
	return nil
}

// OnConnectionEvent implements TiDB plugin's OnFlush SPI.
func OnFlush(_ context.Context, _ *plugin.Manifest) error {
	return global.Update()
}

// OnShutdown implements TiDB plugin's OnShutdown SPI.
func OnShutdown(ctx context.Context, manifest *plugin.Manifest) error {
	return nil
}

// OnConnectionEvent implements TiDB audit plugin's OnConnectionEvent SPI.
func OnConnectionEvent(_ context.Context, event plugin.ConnectionEvent, c *variable.ConnectionInfo) error {
	if event != plugin.PreAuth {
		return nil
	}

	ip, err := net.LookupIP(c.Host)
	if err != nil {
		return err
	}

	whitelist := global.Get()
	if len(whitelist.groups) == 0 {
		// No whitelist data.
		return nil
	}

	for _, group := range whitelist.groups {
		for _, iplist := range group.IPList {
			if iplist.Contains(ip[0]) {
				return nil
			}
		}
	}
	return errors.New("Host " + c.Host + " is not in the IP Whitelist")
}

func Validate(ctx context.Context, m *plugin.Manifest) error {
	return nil
}

type whitelist struct {
	groups []Group
}

type Group struct {
	ID     int64
	Name   string
	IPList []*net.IPNet
}

func (i *whitelist) loadFromTable(sctx sessionctx.Context) error {
	ctx := context.Background()
	tmp, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, selectTableSQL)
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
			err = i.decodeTableRow(row)
			if err != nil {
				log.Error("read whitelist table fail", zap.Error(err))
				continue // ignore this row and continue...
			}
		}
		// NOTE: decodeTableRow decodes data from a chunk Row, that is a shallow copy.
		// The result will reference memory in the chunk, so the chunk must not be reused
		// here, otherwise some werid bug will happen!
		chk = chunk.Renew(chk, sctx.GetSessionVars().MaxChunkSize)
	}
}

func (i *whitelist) decodeTableRow(row chunk.Row) error {
	id := row.GetInt64(0)
	name := row.GetString(1)
	group := Group{ID: id, Name: name}

	list := row.GetString(2)
	iplist := strings.Split(list, ",")
	for _, str := range iplist {
		_, ipNet, err := net.ParseCIDR(str)
		if err != nil {
			log.Error("invalidate ip address", zap.String("ip", str))
			continue // ignore and continue
		}
		group.IPList = append(group.IPList, ipNet)
	}
	if len(group.IPList) > 0 {
		i.groups = append(i.groups, group)
	}
	return nil
}

var global = &handle{}

type handle struct {
	mu struct {
		sync.RWMutex
		dom *domain.Domain
	}
	value atomic.Value
}

// Get the MySQLPrivilege for read.
func (h *handle) Get() *whitelist {
	return h.value.Load().(*whitelist)
}

// Update loads all the privilege info from kv storage.
// The caller should guarantee that dom is not nil.
func (h *handle) Update() error {
	pool := h.mu.dom.SysSessionPool()
	tmp, err := pool.Get()
	if err != nil {
		return err
	}
	defer pool.Put(tmp)

	sctx := tmp.(sessionctx.Context)
	var wl whitelist
	err = wl.loadFromTable(sctx)
	if err != nil {
		return err
	}

	h.value.Store(&wl)
	return nil
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
	rss, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, createTableSQL)
	if err != nil {
		return err
	}
	for _, rs := range rss {
		rs.Close()
	}
	return h.Update()
}
