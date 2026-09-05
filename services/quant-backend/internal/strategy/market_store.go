// 行情加载：池内股票原始 Bar 与指数收盘序列，对齐 levels.md 价格尺度。
//
// 原价与当日复权因子分别保留，不使用区间末因子改写历史；金额统一千元→元。
// 调用方须限月份与天数，单股 LIMIT 兜底；回放前必须校验价格与公司行动支持范围。
package strategy

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
)

// LoadStockBars 加载池内股票原始Bar与当日复权因子，历史价格不受结束日期影响。
func LoadStockBars(q Queryer, codes []string, from, to string, perStockLimit int,
	allowMissing ...bool,
) (map[string]map[string]Bar, error) {
	if len(codes) == 0 {
		return nil, errInvalid("股票代码为空")
	}
	if from == "" || to == "" || from > to {
		return nil, errInvalid("日期区间非法")
	}
	if perStockLimit <= 0 || perStockLimit > 2000 {
		perStockLimit = 800
	}
	out := make(map[string]map[string]Bar, len(codes))
	for _, code := range codes {
		rows, err := q.QueryRows("SELECT trade_date, open, high, low, close, amount, adj_factor"+
			" FROM daily_price WHERE ts_code=? AND trade_date BETWEEN ? AND ?"+
			" ORDER BY trade_date ASC LIMIT ?", code, from, to, perStockLimit+1)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 && (len(allowMissing) == 0 || !allowMissing[0]) {
			return nil, fmt.Errorf("%s: 区间内无行情，回放阻断", code)
		}
		if len(rows) > perStockLimit {
			return nil, fmt.Errorf("%s: 行情超过%d条上限，回放阻断", code, perStockLimit)
		}
		pm := make(map[string]Bar, len(rows))
		for _, r := range rows {
			d := strOf(r["trade_date"])
			if _, err := time.Parse("2006-01-02", d); err != nil || pm[d].Date != "" {
				return nil, fmt.Errorf("%s: 行情日期非法或重复: %q", code, d)
			}
			amount := quoteNumber(r["amount"]) * AmountKUnit
			pm[d] = Bar{Date: d,
				Open:      quoteNumber(r["open"]),
				High:      quoteNumber(r["high"]),
				Low:       quoteNumber(r["low"]),
				Close:     quoteNumber(r["close"]),
				Amount:    amount,
				AdjFactor: quoteNumber(r["adj_factor"]),
			}
		}
		out[code] = pm
	}
	if len(out) == 0 {
		return nil, errInvalid("区间内无可用行情")
	}
	return out, nil
}

// ValidateReplayBars 校验原始量价和复权因子，变化所涉账户权益由runner逐日阻断。
func ValidateReplayBars(bars map[string]map[string]Bar) error {
	if len(bars) == 0 {
		return errInvalid("回放行情为空")
	}
	codes := make([]string, 0, len(bars))
	for code := range bars {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	for _, code := range codes {
		prices := bars[code]
		if len(prices) == 0 {
			return fmt.Errorf("%s: 无有效行情，回放阻断", code)
		}
		dates := make([]string, 0, len(prices))
		for date := range prices {
			dates = append(dates, date)
		}
		sort.Strings(dates)
		for _, date := range dates {
			b := prices[date]
			for _, value := range []float64{b.Open, b.High, b.Low, b.Close, b.AdjFactor} {
				if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
					return fmt.Errorf("%s %s: 原始价格或复权因子缺失/非法，回放阻断", code, date)
				}
			}
			if b.Low > math.Min(b.Open, b.Close) || b.High < math.Max(b.Open, b.Close) ||
				b.Amount < 0 || math.IsNaN(b.Amount) || math.IsInf(b.Amount, 0) {
				return fmt.Errorf("%s %s: OHLC或成交额非法，回放阻断", code, date)
			}
		}
	}
	return nil
}

// LoadIndexCloses 加载指数收盘序列：单指数单区间，LIMIT 按天数上限。
func LoadIndexCloses(q Queryer, code, from, to string, limit int) (map[string]float64, error) {
	if code == "" || from == "" || to == "" || from > to {
		return nil, errInvalid("指数参数非法")
	}
	if limit <= 0 || limit > 2000 {
		limit = 800
	}
	rows, err := q.QueryRows("SELECT trade_date, close FROM market_index WHERE ts_code=?"+
		" AND trade_date BETWEEN ? AND ? ORDER BY trade_date ASC LIMIT ?", code, from, to, limit+1)
	if err != nil {
		return nil, err
	}
	if len(rows) > limit {
		return nil, fmt.Errorf("%s: 指数行情超过%d条上限，回放阻断", code, limit)
	}
	out := make(map[string]float64, len(rows))
	for _, r := range rows {
		d, c := strOf(r["trade_date"]), quoteNumber(r["close"])
		_, duplicate := out[d]
		if _, err := time.Parse("2006-01-02", d); err != nil || duplicate ||
			c <= 0 || math.IsNaN(c) || math.IsInf(c, 0) {
			return nil, fmt.Errorf("%s: 指数日期重复或量价非法: %q", code, d)
		}
		out[d] = c
	}
	if len(out) == 0 {
		return nil, errInvalid("指数区间无数据")
	}
	return out, nil
}

// quoteNumber 严格解析行情数值，非法或缺失返回NaN交给质量校验阻断。
func quoteNumber(value any) float64 {
	if b, ok := value.([]byte); ok {
		value = string(b)
	}
	n, err := strconv.ParseFloat(fmt.Sprint(value), 64)
	if err != nil {
		return math.NaN()
	}
	return n
}

// UnionDates 并集交易日：多股票 Bar 的日期并集升序，用于回放 Dates。
func UnionDates(bars map[string]map[string]Bar, from, to string) []string {
	set := map[string]bool{}
	for _, pm := range bars {
		for d := range pm {
			if d >= from && d <= to {
				set[d] = true
			}
		}
	}
	dates := make([]string, 0, len(set))
	for d := range set {
		dates = append(dates, d)
	}
	sort.Strings(dates)
	return dates
}

// PoolCodes 池内代码并集：多月 Top20 去重，用于批量加载行情。
func PoolCodes(pools map[string]PoolVersion) []string {
	set := map[string]bool{}
	for _, p := range pools {
		for _, e := range p.Entries {
			set[e.TsCode] = true
		}
	}
	codes := make([]string, 0, len(set))
	for c := range set {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	return codes
}

// LoadMarketInputs 加载市场公共指标（全市场宽度、量能比与指数趋势）。
func LoadMarketInputs(q Queryer, from, to string) (map[string]MarketInput, error) {
	rows, err := q.QueryRows("SELECT trade_date, breadth, breadth_prev, breadth_5ago, volume_ratio, index_trend, data_ok"+
		" FROM market_indicators WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date ASC", from, to)
	if err != nil {
		return nil, err
	}
	out := make(map[string]MarketInput, len(rows))
	for _, r := range rows {
		d := strOf(r["trade_date"])
		b := quoteNumber(r["breadth"])
		bPrev := quoteNumber(r["breadth_prev"])
		b5Ago := quoteNumber(r["breadth_5ago"])
		v := quoteNumber(r["volume_ratio"])
		t := int(quoteNumber(r["index_trend"])) == 1
		ok := int(quoteNumber(r["data_ok"])) == 1
		out[d] = MarketInput{
			B:         FactorResult{Value: b},
			BPrev:     FactorResult{Value: bPrev},
			B5Ago:     FactorResult{Value: b5Ago},
			V:         FactorResult{Value: v},
			Trend:     t,
			DataOK:    ok,
			HasD:      false,
			DownFromY: b < bPrev,
		}
	}
	return out, nil
}
