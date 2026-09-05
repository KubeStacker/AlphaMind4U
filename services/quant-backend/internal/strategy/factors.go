// 因子计算：量价核心与市场宽度，对齐 spec/strategy/factors.md。
package strategy

import (
	"math"
)

// Bar 单日OHLC与复权因子：加载时保留原价，因子计算前由调用方确保窗口尺度一致。
type Bar struct {
	Date      string  // 交易日。
	Open      float64 // 开盘。
	High      float64 // 最高。
	Low       float64 // 最低。
	Close     float64 // 收盘。
	Amount    float64 // 成交额（元）。
	AdjFactor float64 // 当日复权因子；不能直接推算现金分红或送股数量。
}

// FactorResult 因子值与缺失原因：缺失时 Value 为 NaN 且 Reason 非空。
type FactorResult struct {
	Value  float64 // 因子值。
	Reason string  // 缺失原因，空表示可用。
}

// bad 缺失构造器。
func bad(reason string) FactorResult { return FactorResult{Value: math.NaN(), Reason: reason} }

// SMA 收盘简单均值：窗口须连续完整，否则返回缺失。
func SMA(closes []float64, n int) FactorResult {
	if len(closes) < n || n <= 0 {
		return bad("窗口不足")
	}
	seg := closes[len(closes)-n:]
	sum := 0.0
	for _, v := range seg {
		if v <= 0 || math.IsNaN(v) {
			return bad("价格非法")
		}
		sum += v
	}
	return FactorResult{Value: sum / float64(n)}
}

// MOM 动量：C_t/C_(t-n)-1，需 n+1 个收盘点。
func MOM(closes []float64, n int) FactorResult {
	if len(closes) < n+1 || n <= 0 {
		return bad("窗口不足")
	}
	now, old := closes[len(closes)-1], closes[len(closes)-1-n]
	if old <= 0 || now <= 0 {
		return bad("价格非法")
	}
	return FactorResult{Value: now/old - 1}
}

// RS20 相对强度：个股 MOM20 减指数 MOM20，同截面减同一指数不改变排序。
func RS20(stock, index []float64, n int) FactorResult {
	s, m := MOM(stock, n), MOM(index, n)
	if !s.Available() {
		return bad("个股" + s.Reason)
	}
	if !m.Available() {
		return bad("指数" + m.Reason)
	}
	return FactorResult{Value: s.Value - m.Value}
}

// Available 因子是否可用。
func (f FactorResult) Available() bool { return f.Reason == "" && !math.IsNaN(f.Value) }

// ATR20 真实波幅均值：需连续 n+1 个 Bar 计算 TR。
func ATR20(bars []Bar, n int) FactorResult {
	if len(bars) < n+1 || n <= 0 {
		return bad("窗口不足")
	}
	seg := bars[len(bars)-n-1:]
	sum := 0.0
	for i := 1; i < len(seg); i++ {
		h, l, pc := seg[i].High, seg[i].Low, seg[i-1].Close
		if h <= 0 || l <= 0 || pc <= 0 || h < l {
			return bad("价格非法")
		}
		tr := math.Max(h-l, math.Max(math.Abs(h-pc), math.Abs(l-pc)))
		sum += tr
	}
	return FactorResult{Value: sum / float64(n)}
}

// HighestClose 前 n 日最高收盘 HC：不含信号日，调用方传入信号日前序列。
func HighestClose(closes []float64, n int) FactorResult {
	if len(closes) < n {
		return bad("窗口不足")
	}
	mx := closes[len(closes)-n]
	for _, v := range closes[len(closes)-n:] {
		if v <= 0 {
			return bad("价格非法")
		}
		if v > mx {
			mx = v
		}
	}
	return FactorResult{Value: mx}
}

// HighestHigh 前 n 日最高价 H：不含信号日，用于 E2 空间观察线。
func HighestHigh(bars []Bar, n int) FactorResult {
	if len(bars) < n {
		return bad("窗口不足")
	}
	mx := bars[len(bars)-n].High
	for _, b := range bars[len(bars)-n:] {
		if b.High <= 0 {
			return bad("价格非法")
		}
		if b.High > mx {
			mx = b.High
		}
	}
	return FactorResult{Value: mx}
}

// AmountRatio 量能比：当日金额 / 前 n 日均值，分母不含当日。
func AmountRatio(amounts []float64, n int) FactorResult {
	if len(amounts) < n+1 {
		return bad("窗口不足")
	}
	sum := 0.0
	for _, v := range amounts[len(amounts)-1-n : len(amounts)-1] {
		if v < 0 {
			return bad("金额非法")
		}
		sum += v
	}
	if sum <= 0 {
		return bad("分母为零")
	}
	return FactorResult{Value: amounts[len(amounts)-1] / (sum / float64(n))}
}

// Extension 均线偏离：(C-MA20)/ATR，ATR<=0 返回缺失。
func Extension(close, ma, atr float64) FactorResult {
	if close <= 0 || ma <= 0 {
		return bad("价格非法")
	}
	if !(atr > 0) {
		return bad("ATR非法")
	}
	return FactorResult{Value: (close - ma) / atr}
}

// Efficiency 趋势效率 ER20：方向行程 / 路径总和，反复震荡趋近 0。
func Efficiency(closes []float64, n int) FactorResult {
	if len(closes) < n+1 {
		return bad("窗口不足")
	}
	seg := closes[len(closes)-1-n:]
	path := 0.0
	for i := 1; i < len(seg); i++ {
		if seg[i] <= 0 || seg[i-1] <= 0 {
			return bad("价格非法")
		}
		path += math.Abs(seg[i] - seg[i-1])
	}
	if path <= 0 {
		return bad("分母为零")
	}
	return FactorResult{Value: math.Abs(seg[len(seg)-1]-seg[0]) / path}
}

// Breadth 市场宽度 B：可交易且 20 日完整者中收盘>MA20 占比，分母不足记不可用。
func Breadth(above, valid int) FactorResult {
	if valid <= 0 {
		return bad("有效样本不足")
	}
	return FactorResult{Value: float64(above) / float64(valid)}
}
