// Package db 独占 SQLite 连接与读写封装。
//
// 全进程只允许一个 DB 实例：底层 *sql.DB 限制单连接，
// 再加互斥锁串行，语义对齐旧 Python 的 RLock 共享连接。
package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// slowQueryMs 慢查询阈值毫秒。
const slowQueryMs = 1000

// slowLog 超阈值记 warn（只记 SQL 前 120 字，不记参数全量）。
func slowLog(start time.Time, op, sql string) {
	if el := time.Since(start); el > slowQueryMs*time.Millisecond {
		s := sql
		if len(s) > 120 {
			s = s[:120]
		}
		log.Printf("慢查询 %.1fs %s: %s", el.Seconds(), op, s)
	}
}

// Row 通用行，TEXT→string、INTEGER→int64、REAL→float64、NULL→nil。
type Row map[string]any

// DB 独占数据库连接。
type DB struct {
	mu sync.Mutex
	db *sql.DB
}

// Open 打开 SQLite 库文件并设置单写者参数。
func Open(path string) (*DB, error) {
	//nolint:revive // 驱动名固定
	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("打开数据库失败: %w", err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	d := &DB{db: sqlDB}
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=10000",
		"PRAGMA cache_size=-64000",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA mmap_size=1073741824",
	} {
		if _, err := d.Exec(pragma); err != nil {
			sqlDB.Close()
			return nil, fmt.Errorf("设置 %s 失败: %w", pragma, err)
		}
	}
	return d, nil
}

// Close 关闭连接。
func (d *DB) Close() error { return d.db.Close() }

// Exec 串行执行写语句，返回影响行数。
func (d *DB) Exec(query string, args ...any) (int64, error) {
	start := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	defer slowLog(start, "exec", query)
	res, err := d.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ExecTx 单事务执行多条语句，任一失败整体回滚。
func (d *DB) ExecTx(stmts []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	for _, s := range stmts {
		if _, err := tx.Exec(s); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// QueryValue 查询单值，不存在返回 ok=false。
func (d *DB) QueryValue(query string, args ...any) (val any, ok bool, err error) {
	start := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	defer slowLog(start, "query1", query)
	row := d.db.QueryRow(query, args...)
	var v any
	if err := row.Scan(&v); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, err
	}
	return v, true, nil
}

// QueryInt 查询单整数，不存在返回 0。
func (d *DB) QueryInt(query string, args ...any) (int64, error) {
	v, ok, err := d.QueryValue(query, args...)
	if err != nil || !ok || v == nil {
		return 0, err
	}
	switch n := v.(type) {
	case int64:
		return n, nil
	case float64:
		return int64(n), nil
	case string:
		var i int64
		_, err := fmt.Sscan(n, &i)
		return i, err
	default:
		return 0, fmt.Errorf("无法转整数: %T", v)
	}
}

// QueryRows 查询多行，列名保持原样。
func (d *DB) QueryRows(query string, args ...any) ([]Row, error) {
	start := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	defer slowLog(start, "queryN", query)
	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make([]Row, 0, 64)
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		r := make(Row, len(cols))
		for i, c := range cols {
			r[c] = vals[i]
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// QueryStrings 查询单列字符串。
func (d *DB) QueryStrings(query string, args ...any) ([]string, error) {
	rows, err := d.QueryRows(query, args...)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		for _, v := range r {
			if s, ok := v.(string); ok {
				out = append(out, s)
			} else if v != nil {
				out = append(out, fmt.Sprint(v))
			}
			break
		}
	}
	return out, nil
}

// fsCacheMu first_seen 列存在性缓存锁。
var fsCacheMu sync.Mutex

// fsCache 表名 → 是否含 first_seen_at。
var fsCache = map[string]bool{}

// quoteIdent 双引号转义标识符。
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// hasFirstSeen 检查表是否有 first_seen_at 列（带缓存，直连驱动不经业务锁）。
func (d *DB) hasFirstSeen(table string) bool {
	fsCacheMu.Lock()
	v, ok := fsCache[table]
	fsCacheMu.Unlock()
	if ok {
		return v
	}
	found := false
	rows, err := d.db.Query("PRAGMA table_info(" + quoteIdent(table) + ")")
	if err == nil {
		defer rows.Close()
		var cid, notnull, pk int
		var name, typ string
		var dflt any
		for rows.Next() {
			if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
				break
			}
			if name == "first_seen_at" {
				found = true
				break
			}
		}
	}
	fsCacheMu.Lock()
	fsCache[table] = found
	fsCacheMu.Unlock()
	return found
}

// tmpName 分批临时表名。
func tmpName(table string, start int) string {
	safe := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		return '_'
	}, table)
	return fmt.Sprintf("tmp_%s_%d", safe, start)
}

// BulkUpsert 批量 upsert：先 UPDATE 非键列，再 INSERT 缺失键，分块提交。
//
// 语义对齐旧 art_safe_write：调用前先由 Dedupe 去重，避免主键冲突。
// 锁粒度为单批次：批次之间释放锁，读查询可穿插，不饿死接口。
// 含 first_seen_at 列的表，INSERT 分支自动写入首次采集时间（UPDATE 不碰）。
func (d *DB) BulkUpsert(table string, cols, keys []string, rows []Row, chunk int) error {
	if len(rows) == 0 {
		return nil
	}
	if chunk <= 0 || chunk > 10000 {
		chunk = 6000
	}
	quoted := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
	colCSV := make([]string, len(cols))
	for i, c := range cols {
		colCSV[i] = quoted(c)
	}
	keyCSV := make([]string, len(keys))
	for i, k := range keys {
		keyCSV[i] = quoted(k)
	}
	nonKeys := make([]string, 0, len(cols))
	for _, c := range cols {
		skip := false
		for _, k := range keys {
			if c == k {
				skip = true
				break
			}
		}
		if !skip {
			nonKeys = append(nonKeys, c)
		}
	}
	matchOp := "="
	if table == "stock_dividend" {
		matchOp = " IS " // 官方提议阶段可无公告日，NULL键也必须幂等匹配。
	}
	joinPred := make([]string, len(keys))
	for i, k := range keys {
		joinPred[i] = "t." + quoted(k) + matchOp + "v." + quoted(k)
	}
	joinOn := strings.Join(joinPred, " AND ")
	var setClause string
	if len(nonKeys) > 0 {
		sets := make([]string, len(nonKeys))
		for i, c := range nonKeys {
			sets[i] = quoted(c) + "=v." + quoted(c)
		}
		setClause = strings.Join(sets, ", ")
	}
	selCSV := make([]string, len(cols))
	for i, c := range cols {
		selCSV[i] = "v." + quoted(c)
	}
	if d.hasFirstSeen(table) {
		cols = append(cols, "first_seen_at")
		colCSV = append(colCSV, quoted("first_seen_at"))
		selCSV = append(selCSV, "strftime('%Y-%m-%d %H:%M:%S','now')")
	}

	start := time.Now()
	defer slowLog(start, "upsert", "BulkUpsert "+table)
	for off := 0; off < len(rows); off += chunk {
		end := off + chunk
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[off:end]
		d.mu.Lock()
		err := d.bulkUpsertBatch(tmpName(table, off), table, cols, colCSV, keyCSV,
			setClause, joinOn, selCSV, batch)
		d.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// bulkUpsertBatch 单批次 upsert（调用者已持锁）。
func (d *DB) bulkUpsertBatch(tmp, table string, cols, colCSV, keyCSV []string,
	setClause, joinOn string, selCSV []string, batch []Row,
) error {
	_ = keyCSV
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = c + " TEXT"
	}
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	rollback := func() { _ = tx.Rollback() }
	if _, err := tx.Exec("CREATE TEMP TABLE " + tmp + " (" + strings.Join(defs, ", ") + ")"); err != nil {
		rollback()
		return err
	}
	holders := make([]string, len(cols))
	for i := range holders {
		holders[i] = "?"
	}
	stmt, err := tx.Prepare("INSERT INTO " + tmp + " (" + strings.Join(colCSV, ", ") +
		") VALUES (" + strings.Join(holders, ", ") + ")")
	if err != nil {
		rollback()
		return err
	}
	for _, r := range batch {
		args := make([]any, len(cols))
		for i, c := range cols {
			args[i] = toSQLiteValue(r[c])
		}
		if _, err := stmt.Exec(args...); err != nil {
			_ = stmt.Close()
			rollback()
			return err
		}
	}
	_ = stmt.Close()
	if setClause != "" {
		if _, err := tx.Exec("UPDATE " + table + " AS t SET " + setClause +
			" FROM " + tmp + " AS v WHERE " + joinOn); err != nil {
			rollback()
			return err
		}
	}
	if _, err := tx.Exec("INSERT INTO " + table + " (" + strings.Join(colCSV, ", ") + ") SELECT " +
		strings.Join(selCSV, ", ") + " FROM " + tmp + " AS v WHERE NOT EXISTS (SELECT 1 FROM " +
		table + " AS t WHERE " + joinOn + ")"); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec("DROP TABLE " + tmp); err != nil {
		rollback()
		return err
	}
	return tx.Commit()
}

// toSQLiteValue 转换 Go 值为 SQLite 可存类型。
func toSQLiteValue(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		if t == "" {
			return nil
		}
		return t
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return int64(t)
	case int64:
		return t
	case bool:
		if t {
			return int64(1)
		}
		return int64(0)
	default:
		s := fmt.Sprint(v)
		if s == "" || s == "<nil>" {
			return nil
		}
		return s
	}
}

// ReplaceAll 全量替换：单事务内 DELETE + 去重 INSERT，调用前已 Dedupe。
func (d *DB) ReplaceAll(table string, cols []string, rows []Row, chunk int) error {
	if chunk <= 0 || chunk > 5000 {
		chunk = 2000
	}
	quoted := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
	allCols := append([]string{}, cols...)
	if d.hasFirstSeen(table) {
		allCols = append(allCols, "first_seen_at")
	}
	colCSV := make([]string, len(allCols))
	for i, c := range allCols {
		colCSV[i] = quoted(c)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM " + table); err != nil {
		_ = tx.Rollback()
		return err
	}
	defs := make([]string, len(allCols))
	for i, c := range allCols {
		defs[i] = c + " TEXT"
	}
	if _, err := tx.Exec("CREATE TEMP TABLE tmp_replace (" + strings.Join(defs, ", ") + ")"); err != nil {
		_ = tx.Rollback()
		return err
	}
	holders := make([]string, len(allCols))
	for i := range holders {
		holders[i] = "?"
	}
	stmt, err := tx.Prepare("INSERT INTO tmp_replace (" + strings.Join(colCSV, ", ") +
		") VALUES (" + strings.Join(holders, ", ") + ")")
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for start := 0; start < len(rows); start += chunk {
		end := start + chunk
		if end > len(rows) {
			end = len(rows)
		}
		for _, r := range rows[start:end] {
			args := make([]any, len(allCols))
			for i, c := range allCols {
				args[i] = toSQLiteValue(r[c])
			}
			if _, err := stmt.Exec(args...); err != nil {
				_ = stmt.Close()
				_ = tx.Rollback()
				return err
			}
		}
	}
	_ = stmt.Close()
	sel := make([]string, len(allCols))
	for i, c := range allCols {
		if c == "first_seen_at" {
			sel[i] = "strftime('%Y-%m-%d %H:%M:%S','now')"
			continue
		}
		sel[i] = "v." + quoted(c)
	}
	if _, err := tx.Exec("INSERT INTO " + table + " (" + strings.Join(colCSV, ", ") +
		") SELECT " + strings.Join(sel, ", ") + " FROM tmp_replace AS v"); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.Exec("DROP TABLE tmp_replace"); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Dedupe 按键保留最后一行，去重后返回新切片。
func Dedupe(rows []Row, keys []string) []Row {
	seen := make(map[string]int, len(rows))
	out := make([]Row, 0, len(rows))
	for _, r := range rows {
		var b strings.Builder
		for _, k := range keys {
			b.WriteString(fmt.Sprintf("%v\x00", r[k]))
		}
		sig := b.String()
		if idx, dup := seen[sig]; dup {
			out[idx] = r
			continue
		}
		seen[sig] = len(out)
		out = append(out, r)
	}
	return out
}
