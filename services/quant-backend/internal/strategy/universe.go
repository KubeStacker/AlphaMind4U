// 股票池：上月自然月全 A 成交额 Top20，对齐 spec/strategy/universe.md。
package strategy

import (
	"fmt"
	"sort"
)

// DailyAmount 单日成交额输入：金额统一为元，复权不影响金额汇总。
type DailyAmount struct {
	TsCode    string  // 证券代码。
	TradeDate string  // 交易日 YYYY-MM-DD。
	Amount    float64 // 当日成交金额（元）。
}

// PoolEntry 月度排名条目：名次、累计金额，同额按代码升序。
type PoolEntry struct {
	Rank   int     // 1~20 名次。
	TsCode string  // 证券代码。
	Amount float64 // 自然月累计成交额（元）。
}

// PoolVersion 股票池版本：月份、日期范围、生成信息，用于快照复算。
type PoolVersion struct {
	Month     string      // 生效月份 YYYY-MM。
	RangeFrom string      // 排名日期下限。
	RangeTo   string      // 排名日期上限。
	Entries   []PoolEntry // 原始 Top20。
	DatasetV  string      // 数据版本标识。
}

// BuildMonthlyTop20 按自然月累计成交额排名取 Top20，不过滤不递补。
func BuildMonthlyTop20(month string, from, to string, rows []DailyAmount, datasetV string) PoolVersion {
	sum := map[string]float64{}
	for _, r := range rows {
		if r.TsCode == "" || r.Amount <= 0 {
			continue
		}
		sum[r.TsCode] += r.Amount
	}
	all := make([]PoolEntry, 0, len(sum))
	for code, amt := range sum {
		all = append(all, PoolEntry{TsCode: code, Amount: amt})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Amount != all[j].Amount {
			return all[i].Amount > all[j].Amount
		}
		return all[i].TsCode < all[j].TsCode
	})
	if len(all) > 20 {
		all = all[:20]
	}
	for i := range all {
		all[i].Rank = i + 1
	}
	return PoolVersion{Month: month, RangeFrom: from, RangeTo: to, Entries: all, DatasetV: datasetV}
}

// Qualify 交易资格过滤结果：保留原始排名，只标记可买与原因。
type Qualify struct {
	TsCode  string // 证券代码。
	Allowed bool   // 是否允许新开仓。
	Reason  string // 禁止原因：权限/ST/停牌/窗口不足/最小买量等。
}

// FilterPool 资格过滤：不足 20 只保留缺位，不递补第 21 名。
func FilterPool(pool PoolVersion, quals map[string]Qualify) (tradable []PoolEntry, missing int) {
	for _, e := range pool.Entries {
		q, ok := quals[e.TsCode]
		if !ok || q.Allowed {
			tradable = append(tradable, e)
			continue
		}
		missing++
	}
	return tradable, missing
}

// MonthKey 由交易日取自然月 YYYY-MM。
func MonthKey(tradeDate string) string {
	if len(tradeDate) >= 7 {
		return tradeDate[:7]
	}
	return ""
}

// PrevMonth 上一完整自然月：输入 YYYY-MM-DD 返回 YYYY-MM。
func PrevMonth(tradeDate string) string {
	var y, m, d int
	if _, err := fmt.Sscanf(tradeDate, "%d-%d-%d", &y, &m, &d); err != nil {
		return ""
	}
	m--
	if m < 1 {
		m, y = 12, y-1
	}
	return fmt.Sprintf("%04d-%02d", y, m)
}
