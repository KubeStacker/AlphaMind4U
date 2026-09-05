// 股票池 DB 重建：从 daily_price 按自然月汇总 Top20，对齐 universe.md。
package strategy

import (
	"fmt"
	"sort"
	"time"

	"quantbackend/internal/db"
)

// AmountKUnit 库内成交额单位换算：Tushare daily 金额为千元，策略统一为元。
const AmountKUnit = 1000.0

// Queryer 只读查询最小接口，便于单测伪造行。
type Queryer interface {
	QueryRows(query string, args ...any) ([]db.Row, error)
}

// MonthRange 月内交易日边界：最早最晚日期与去重交易日数。
type MonthRange struct {
	From string // 月内最早交易日。
	To   string // 月内最晚交易日。
	Days int    // 去重交易日数。
}

// MonthDates 查某自然月 daily_price 的日期边界与天数，LIMIT 1 聚合查询。
func MonthDates(q Queryer, yearMonth string) (MonthRange, error) {
	from, to, err := monthBounds(yearMonth)
	if err != nil {
		return MonthRange{}, err
	}
	rows, err := q.QueryRows("SELECT MIN(trade_date) AS dmin, MAX(trade_date) AS dmax,"+
		" COUNT(DISTINCT trade_date) AS dnum FROM daily_price WHERE trade_date>=? AND trade_date<? LIMIT 1", from, to)
	if err != nil {
		return MonthRange{}, err
	}
	if len(rows) == 0 {
		return MonthRange{}, errInvalid("该月无行情")
	}
	return MonthRange{From: strOf(rows[0]["dmin"]), To: strOf(rows[0]["dmax"]), Days: intOf(rows[0]["dnum"])}, nil
}

// FetchMonthlyTop20 重建上月 Top20：SQL 内按累计金额倒序、同额代码升序取 20。
func FetchMonthlyTop20(q Queryer, effectMonth, rankMonth string, datasetV string) (PoolVersion, error) {
	from, to, err := monthBounds(rankMonth)
	if err != nil {
		return PoolVersion{}, err
	}
	mr, err := MonthDates(q, rankMonth)
	if err != nil {
		return PoolVersion{}, err
	}
	if mr.Days == 0 {
		return PoolVersion{}, errInvalid("排名月无交易日")
	}
	rows, err := q.QueryRows("SELECT ts_code AS ts_code, SUM(amount) AS sum_amt FROM daily_price"+
		" WHERE trade_date>=? AND trade_date<? GROUP BY ts_code ORDER BY sum_amt DESC, ts_code ASC LIMIT 20", from, to)
	if err != nil {
		return PoolVersion{}, err
	}
	pool := PoolVersion{Month: effectMonth, RangeFrom: mr.From, RangeTo: mr.To, DatasetV: datasetV}
	for i, r := range rows {
		amt := floatOf(r["sum_amt"]) * AmountKUnit
		if amt <= 0 {
			continue
		}
		pool.Entries = append(pool.Entries, PoolEntry{Rank: i + 1, TsCode: strOf(r["ts_code"]), Amount: amt})
	}
	if len(pool.Entries) == 0 {
		return PoolVersion{}, errInvalid("Top20 为空")
	}
	// Go 侧二次排序兜底：金额倒序、同额代码升序，不完全依赖 SQL 排序口径。
	sort.Slice(pool.Entries, func(i, j int) bool {
		if pool.Entries[i].Amount != pool.Entries[j].Amount {
			return pool.Entries[i].Amount > pool.Entries[j].Amount
		}
		return pool.Entries[i].TsCode < pool.Entries[j].TsCode
	})
	// 排名按返回顺序重编，保证 1..N 连续。
	for i := range pool.Entries {
		pool.Entries[i].Rank = i + 1
	}
	return pool, nil
}

// monthBounds 返回自然月半开区间，使日期索引可用于范围查询。
func monthBounds(month string) (string, string, error) {
	d, err := time.Parse("2006-01", month)
	if err != nil {
		return "", "", errInvalid("排名月份非法")
	}
	return d.Format("2006-01-02"), d.AddDate(0, 1, 0).Format("2006-01-02"), nil
}

// strOf 行值转字符串。
func strOf(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprint(v)
}

// floatOf 行值转浮点，非法返回 0。
func floatOf(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case []byte:
		var f float64
		_, _ = fmt.Sscan(string(n), &f)
		return f
	case string:
		var f float64
		_, _ = fmt.Sscan(n, &f)
		return f
	default:
		var f float64
		_, _ = fmt.Sscan(fmt.Sprint(v), &f)
		return f
	}
}

// intOf 行值转整数，非法返回 0。
func intOf(v any) int {
	switch n := v.(type) {
	case int64:
		return int(n)
	case float64:
		return int(n)
	case []byte:
		var i int
		_, _ = fmt.Sscan(string(n), &i)
		return i
	default:
		var i int
		_, _ = fmt.Sscan(fmt.Sprint(v), &i)
		return i
	}
}
