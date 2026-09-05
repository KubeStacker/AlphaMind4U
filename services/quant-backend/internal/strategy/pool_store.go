// 股票池快照存取：重建结果落库与复算读取，对齐 universe.md 修订不倒改。
package strategy

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"quantbackend/internal/db"
)

// PoolStore 快照读写最小接口，*db.DB 直接满足，便于单测伪造。
type PoolStore interface {
	Queryer
	Exec(query string, args ...any) (int64, error)
	BulkUpsert(table string, cols, keys []string, rows []db.Row, chunk int) error
}

// FreezePool 为完整月池计算内容版本，不声称包含原始行情历史版本。
func FreezePool(pool PoolVersion) (PoolVersion, error) {
	if _, err := time.Parse("2006-01", pool.Month); err != nil || len(pool.Entries) == 0 || len(pool.Entries) > 20 {
		return PoolVersion{}, errInvalid("快照月份或明细非法")
	}
	codes := make(map[string]bool, len(pool.Entries))
	for i, entry := range pool.Entries {
		if entry.Rank != i+1 || entry.TsCode == "" || codes[entry.TsCode] ||
			entry.Amount <= 0 || math.IsNaN(entry.Amount) || math.IsInf(entry.Amount, 0) {
			return PoolVersion{}, errInvalid("快照排名、证券或成交额非法")
		}
		if i > 0 {
			prev := pool.Entries[i-1]
			if prev.Amount < entry.Amount || (prev.Amount == entry.Amount && prev.TsCode > entry.TsCode) {
				return PoolVersion{}, errInvalid("快照排序非法")
			}
		}
		codes[entry.TsCode] = true
	}
	pool.DatasetV = ""
	body, err := json.Marshal(pool)
	if err != nil {
		return PoolVersion{}, err
	}
	pool.DatasetV = fmt.Sprintf("pool-sha256:%x", sha256.Sum256(body))
	return pool, nil
}

// SavePool 原子追加完整月份快照，相邻相同内容幂等，修订不覆盖旧发布记录。
func SavePool(s PoolStore, pool PoolVersion) error {
	pool, err := FreezePool(pool)
	if err != nil {
		return err
	}
	body, err := json.Marshal(pool)
	if err != nil {
		return err
	}
	_, err = s.Exec("INSERT INTO strategy_pool_snapshot (effect_month,version,payload_json) SELECT ?,?,?"+
		" WHERE COALESCE((SELECT version FROM strategy_pool_snapshot WHERE effect_month=?"+
		" ORDER BY id DESC LIMIT 1),'')<>?", pool.Month, pool.DatasetV, string(body), pool.Month, pool.DatasetV)
	return err
}

// LoadPool 读取月份最新完整版本并校验内容，无新版返回ok=false。
func LoadPool(s PoolStore, effectMonth string) (pool PoolVersion, ok bool, err error) {
	rows, err := s.QueryRows("SELECT version,payload_json FROM strategy_pool_snapshot"+
		" WHERE effect_month=? ORDER BY id DESC LIMIT 1", effectMonth)
	if err != nil || len(rows) == 0 {
		return PoolVersion{}, false, err
	}
	if err := json.Unmarshal([]byte(strOf(rows[0]["payload_json"])), &pool); err != nil {
		return PoolVersion{}, false, err
	}
	frozen, err := FreezePool(pool)
	if err != nil {
		return PoolVersion{}, false, err
	}
	if pool.Month != effectMonth || pool.DatasetV != frozen.DatasetV || pool.DatasetV != strOf(rows[0]["version"]) {
		return PoolVersion{}, false, errInvalid("月池版本与内容不一致")
	}
	return pool, true, nil
}
