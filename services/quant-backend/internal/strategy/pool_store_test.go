// 快照存取单测：内存库建表、迁移、保存、读取、幂等，不碰生产库。
package strategy

import (
	"strings"
	"testing"

	"quantbackend/internal/db"
)

// memDB 内存库并建表。
func memDB(t *testing.T) *db.DB {
	t.Helper()
	d, err := db.Open(":memory:")
	if err != nil {
		t.Skip("sqlite 不可用")
	}
	t.Cleanup(func() { d.Close() })
	if err := d.InitSchema(); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	return d
}

// TestMigrateApplies0002 迁移 0002 在新库可应用且幂等跳过。
func TestMigrateApplies0002(t *testing.T) {
	d := memDB(t)
	applied, err := d.Migrate()
	if err != nil {
		t.Fatalf("迁移失败: %v", err)
	}
	found := false
	for _, v := range applied {
		if v == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("0002 未应用: %v", applied)
	}
	if again, err := d.Migrate(); err != nil || len(again) != 0 {
		t.Fatalf("重复迁移应跳过: %v %v", again, err)
	}
}

// TestPoolRoundTrip 快照保存读取往返，重存幂等行数不变。
func TestPoolRoundTrip(t *testing.T) {
	d := memDB(t)
	if _, err := d.Migrate(); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}
	pool := PoolVersion{Month: "2026-09", RangeFrom: "2026-08-01", RangeTo: "2026-08-29",
		DatasetV: "vtest", Entries: []PoolEntry{
			{Rank: 1, TsCode: "000001.SZ", Amount: 15000000},
			{Rank: 2, TsCode: "000002.SZ", Amount: 10000000},
		}}
	if err := SavePool(d, pool); err != nil {
		t.Fatalf("保存失败: %v", err)
	}
	got, ok, err := LoadPool(d, "2026-09")
	if err != nil || !ok {
		t.Fatalf("读取失败: %v %v", ok, err)
	}
	if len(got.Entries) != 2 || got.Entries[0].TsCode != "000001.SZ" || got.Entries[0].Amount != 15000000 {
		t.Fatalf("往返不一致: %+v", got)
	}
	if !strings.HasPrefix(got.DatasetV, "pool-sha256:") || got.RangeTo != "2026-08-29" {
		t.Fatalf("快照头不一致: %+v", got)
	}
	if err := SavePool(d, pool); err != nil {
		t.Fatalf("重存失败: %v", err)
	}
	n, _ := d.QueryInt("SELECT COUNT(*) FROM strategy_pool_snapshot WHERE effect_month='2026-09'")
	if n != 1 {
		t.Fatalf("幂等失败，残留 %d 行", n)
	}
	if _, ok, _ := LoadPool(d, "2026-10"); ok {
		t.Fatalf("无快照月份应返回 ok=false")
	}
	if err := SavePool(d, PoolVersion{}); err == nil {
		t.Fatalf("空快照应报错")
	}
}
