package main

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/sqlexec"
	"strconv"
	"strings"
	"time"
)

type mysqlLogRecord struct {
	colNames []string
	values   []interface{}
}

func auditTable(ctx context.Context, eventClass, eventSubClass string, statusCode int) logTrait {
	now, id := auditID()
	l := &mysqlLogRecord{}
	costTime := time.Duration(0)
	if start := ctx.Value("ExecStartTime"); start != nil {
		costTime = time.Since(start.(time.Time))
	}
	l.Add("id", id)
	l.Add("time", now)
	l.Add("event", eventClass)
	l.Add("sub_event", eventSubClass)
	l.Add("status_code", int64(statusCode))
	l.Add("cost_time", float64(costTime)/float64(time.Microsecond))
	return l
}

func (l *mysqlLogRecord) addConn(connInfo *variable.ConnectionInfo, reason string) logTrait {
	l.Add("host", connInfo.Host)
	l.Add("client_ip", connInfo.ClientIP)
	l.Add("user", connInfo.User)
	l.Add("dbs", connInfo.DB)
	l.Add("tables", "")
	l.Add("statement", "")
	l.Add("affect_rows", int64(0))
	l.Add("client_port", connInfo.ClientPort)
	l.Add("connection_id", uint64(connInfo.ConnectionID))
	l.Add("connection_type", connInfo.ConnectionType)
	l.Add("server_id", int64(connInfo.ServerID))
	l.Add("server_port", int64(connInfo.ServerPort))
	l.Add("server_os_login_user", connInfo.ServerOSLoginUser)
	l.Add("server_os_version", connInfo.OSVersion)
	l.Add("client_version", connInfo.ClientVersion)
	l.Add("server_version", connInfo.ServerVersion)
	l.Add("pid", int64(connInfo.PID))
	l.Add("reason", reason)
	return l
}

func (l *mysqlLogRecord) addGeneral(connInfo *variable.ConnectionInfo, dbNames map[string]struct{}, tableNames []string, normalized string, sctx *variable.SessionVars, cmd string) logTrait {
	l.Add("host", connInfo.Host)
	l.Add("client_ip", connInfo.ClientIP)
	l.Add("user", connInfo.User)
	l.Add("dbs", strings.Join(setToSlice(dbNames), ","))
	l.Add("tables", strings.Join(tableNames, ","))
	l.Add("statement", normalized)
	l.Add("affect_rows", sctx.StmtCtx.AffectedRows())
	l.Add("connection_id", uint64(connInfo.ConnectionID))
	l.Add("client_port", connInfo.ClientPort)
	l.Add("pid", int64(connInfo.PID))
	return l
}

func (l *mysqlLogRecord) Add(name string, val interface{}) {
	l.colNames = append(l.colNames, name)
	l.values = append(l.values, val)
}

func (l *mysqlLogRecord) log() {
	pool := global.getDomain().SysSessionPool()
	sctx, err := pool.Get()
	if err != nil {
		return
	}
	defer pool.Put(sctx)
	var qs []string
	for range l.colNames {
		qs = append(qs, "?")
	}
	sqlTmpl := fmt.Sprintf("insert into mysql.tidb_audit_log (%s) values (%s)", strings.Join(l.colNames, ", "), strings.Join(qs, ", "))
	sql, err := interpolateParams(sqlTmpl, l.values)
	if err != nil {
		return
	}
	rss, err := sctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	if err != nil {
		return
	}
	for _, rs := range rss {
		_ = rs.Close()
	}
}

const digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
const digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"

func interpolateParams(query string, args []interface{}) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}

	var buf []byte
	buf = buf[:0]
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint64:
			// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
			buf = strconv.AppendUint(buf, v, 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				v := v.In(time.Local)
				v = v.Add(time.Nanosecond * 500) // To round under microsecond
				year := v.Year()
				year100 := year / 100
				year1 := year % 100
				month := v.Month()
				day := v.Day()
				hour := v.Hour()
				minute := v.Minute()
				second := v.Second()
				micro := v.Nanosecond() / 1000

				buf = append(buf, []byte{
					'\'',
					digits10[year100], digits01[year100],
					digits10[year1], digits01[year1],
					'-',
					digits10[month], digits01[month],
					'-',
					digits10[day], digits01[day],
					' ',
					digits10[hour], digits01[hour],
					':',
					digits10[minute], digits01[minute],
					':',
					digits10[second], digits01[second],
				}...)

				if micro != 0 {
					micro10000 := micro / 10000
					micro100 := micro / 100 % 100
					micro1 := micro % 100
					buf = append(buf, []byte{
						'.',
						digits10[micro10000], digits01[micro10000],
						digits10[micro100], digits01[micro100],
						digits10[micro1], digits01[micro1],
					}...)
				}
				buf = append(buf, '\'')
			}
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				buf = escapeBytesQuotes(buf, v)
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			buf = escapeStringQuotes(buf, v)
			buf = append(buf, '\'')
		default:
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return string(buf), nil
}

// escapeBytesQuotes escapes apostrophes in []byte by doubling them up.
// This escapes the contents of a string by doubling up any apostrophes that
// it contains. This is used when the NO_BACKSLASH_ESCAPES SQL_MODE is in
// effect on the server.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L963-L1038
func escapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringQuotes is similar to escapeBytesQuotes but for string.
func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}
