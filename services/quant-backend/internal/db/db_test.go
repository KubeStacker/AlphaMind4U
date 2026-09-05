// DB 批量写入与事务语义测试（内存库）。
package db

import "testing"

// TestBulkUpsertIdempotent 同一批次重跑结果一致（幂等）。
func TestBulkUpsertIdempotent(t *testing.T) {
	d, err := Open(":memory:")
	if err != nil {
		t.Skip("sqlite 不可用")
	}
	defer d.Close()
	if _, err := d.Exec(`CREATE TABLE t (a TEXT, b TEXT, PRIMARY KEY (a))`); err != nil {
		t.Fatal(err)
	}
	cols, keys := []string{"a", "b"}, []string{"a"}
	rows := []Row{{"a": "1", "b": "x"}, {"a": "1", "b": "y"}}
	rows = Dedupe(rows, keys)
	if err := d.BulkUpsert("t", cols, keys, rows, 100); err != nil {
		t.Fatal(err)
	}
	if err := d.BulkUpsert("t", cols, keys, rows, 100); err != nil {
		t.Fatal(err)
	}
	n, err := d.QueryInt(`SELECT COUNT(*) FROM t`)
	if err != nil || n != 1 {
		t.Fatalf("want 1 got %d err %v", n, err)
	}
	v, _, _ := d.QueryValue(`SELECT b FROM t WHERE a='1'`)
	if v != "y" {
		t.Fatalf("后写应覆盖，got %v", v)
	}
}

// TestBulkUpsertFirstSeen 含 first_seen_at 列的表 INSERT 自动打标。
func TestBulkUpsertFirstSeen(t *testing.T) {
	d, err := Open(":memory:")
	if err != nil {
		t.Skip("sqlite 不可用")
	}
	defer d.Close()
	if _, err := d.Exec(`CREATE TABLE f (a TEXT PRIMARY KEY, first_seen_at TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := d.BulkUpsert("f", []string{"a"}, []string{"a"}, []Row{{"a": "1"}}, 100); err != nil {
		t.Fatal(err)
	}
	v, ok, err := d.QueryValue(`SELECT first_seen_at FROM f WHERE a='1'`)
	if err != nil || !ok || v == nil || v == "" {
		t.Fatalf("first_seen_at 未打标: %v %v %v", v, ok, err)
	}
}

// TestExecTxRollback 任一失败整体回滚。
func TestExecTxRollback(t *testing.T) {
	d, err := Open(":memory:")
	if err != nil {
		t.Skip("sqlite 不可用")
	}
	defer d.Close()
	if _, err := d.Exec(`CREATE TABLE r (a TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	err = d.ExecTx([]string{
		`INSERT INTO r (a) VALUES ('1')`,
		`INSERT INTO 不存在的表 (a) VALUES ('2')`,
	})
	if err == nil {
		t.Fatalf("应返回错误")
	}
	n, _ := d.QueryInt(`SELECT COUNT(*) FROM r`)
	if n != 0 {
		t.Fatalf("回滚失败，残留 %d 行", n)
	}
}

// TestReplaceAllAtomic 全量替换行数一致。
func TestReplaceAllAtomic(t *testing.T) {
	d, err := Open(":memory:")
	if err != nil {
		t.Skip("sqlite 不可用")
	}
	defer d.Close()
	if _, err := d.Exec(`CREATE TABLE m (a TEXT, b TEXT, PRIMARY KEY (a, b))`); err != nil {
		t.Fatal(err)
	}
	rows := []Row{{"a": "1", "b": "x"}, {"a": "2", "b": "y"}}
	if err := d.ReplaceAll("m", []string{"a", "b"}, rows, 100); err != nil {
		t.Fatal(err)
	}
	n, _ := d.QueryInt(`SELECT COUNT(*) FROM m`)
	if n != 2 {
		t.Fatalf("want 2 got %d", n)
	}
}
