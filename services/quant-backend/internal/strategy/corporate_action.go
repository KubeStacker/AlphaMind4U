// 公司行动支持：除权除息、分红派息、送转股及跨日挂单取消。
package strategy

import (
	"fmt"
	"math"
)

// CorporateAction 描述分红派息与送转股。
type CorporateAction struct {
	TsCode     string  `json:"ts_code"`
	ExDate     string  `json:"ex_date"`
	RecordDate string  `json:"record_date"`
	PayDate    string  `json:"pay_date"`
	CashDiv    float64 `json:"cash_div"` // 每股分红金额（元）
	StkDiv     float64 `json:"stk_div"`  // 每股送转比例（送股+转增）
}

// actionGuard 保留最近复权因子与曾买入记录，检测除权除息日变化。
type actionGuard struct {
	last   map[string]float64
	bought map[string]bool
}

// newActionGuard 创建空仓开始的回放校验器。
func newActionGuard() actionGuard {
	return actionGuard{last: map[string]float64{}, bought: map[string]bool{}}
}

// observe 检查复权因子合法性并检测因子是否发生跳变。
func (g *actionGuard) observe(code, date string, factor float64) (bool, float64, error) {
	if factor <= 0 || math.IsNaN(factor) || math.IsInf(factor, 0) {
		return false, 0, fmt.Errorf("%s %s: 复权因子缺失/非法，回放阻断", code, date)
	}
	previous := g.last[code]
	g.last[code] = factor
	if previous == 0 || previous == factor {
		return false, previous, nil
	}
	return true, previous, nil
}

// LoadCorporateActions 加载指定股票池在日期区间内的除权除息实施事件。
func LoadCorporateActions(q Queryer, codes []string, from, to string) (map[string]map[string]CorporateAction, error) {
	out := make(map[string]map[string]CorporateAction, len(codes))
	for _, c := range codes {
		out[c] = map[string]CorporateAction{}
		rows, err := q.QueryRows("SELECT ts_code, ex_date, COALESCE(record_date, '') as record_date,"+
			" COALESCE(pay_date, '') as pay_date, COALESCE(cash_div, 0) as cash_div,"+
			" COALESCE(stk_div, 0) + COALESCE(stk_bo_rate, 0) as stk_div"+
			" FROM stock_dividend WHERE ts_code=? AND div_proc='实施' AND ex_date BETWEEN ? AND ?"+
			" ORDER BY first_seen_at ASC", c, from, to)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			ex := strOf(r["ex_date"])
			out[c][ex] = CorporateAction{
				TsCode:     c,
				ExDate:     ex,
				RecordDate: strOf(r["record_date"]),
				PayDate:    strOf(r["pay_date"]),
				CashDiv:    quoteNumber(r["cash_div"]),
				StkDiv:     quoteNumber(r["stk_div"]),
			}
		}
	}
	return out, nil
}

// ApplyCorporateAction 在除权除息日调整持仓、现金与退出线，满足守恒约束。
func ApplyCorporateAction(
	led *Ledger, code, date string, oldFactor, newFactor float64,
	act CorporateAction, entryATR map[string]float64,
) error {
	h, held := led.Holdings[code]
	if !held || h.Shares <= 0 {
		return nil
	}
	scale := oldFactor / newFactor
	if scale <= 0 || math.IsNaN(scale) || math.IsInf(scale, 0) {
		return fmt.Errorf("%s %s: 复权换算比例非法: %f", code, date, scale)
	}
	// 1. 均价、退出线、最高价及入场ATR等比缩放
	h.AvgCost *= scale
	h.Stop *= scale
	h.High *= scale
	if atr, ok := entryATR[code]; ok && atr > 0 {
		entryATR[code] = atr * scale
	}
	// 2. 送转股增加数量
	bonusShares := 0
	if act.StkDiv > 0 {
		bonusShares = int(math.Floor(float64(h.Shares)*act.StkDiv + 0.0001))
		h.Shares += bonusShares
	}
	// 3. 现金分红到账
	cashDivTotal := 0.0
	if act.CashDiv > 0 {
		origShares := h.Shares - bonusShares
		cashDivTotal = float64(origShares) * act.CashDiv
		led.Cash += cashDivTotal
		led.DividendsReceived += cashDivTotal
	}
	// 4. 记录公司行动流水
	led.recordCorporateAction(date, code, cashDivTotal, bonusShares)
	return nil
}

// CancelPendingBuysForCode 跨权益日的新买单首版取消后重新评估。
func CancelPendingBuysForCode(buys []pendingBuy, code string) ([]pendingBuy, int) {
	out := make([]pendingBuy, 0, len(buys))
	canceled := 0
	for _, b := range buys {
		if b.TsCode == code {
			canceled++
		} else {
			out = append(out, b)
		}
	}
	return out, canceled
}

// signalBars 复制截至信号日的必要窗口，所有历史价格统一到信号日原价尺度。
func signalBars(series map[string]Bar, dates []string, idx, n int) (map[string]Bar, bool) {
	if idx < 0 || idx >= len(dates) || n <= 0 || n > idx+1 {
		return nil, false
	}
	anchor := series[dates[idx]].AdjFactor
	if anchor <= 0 || math.IsNaN(anchor) || math.IsInf(anchor, 0) {
		return nil, false
	}
	out := make(map[string]Bar, n)
	for _, date := range dates[idx-n+1 : idx+1] {
		b, ok := series[date]
		if !ok || b.AdjFactor <= 0 || math.IsNaN(b.AdjFactor) || math.IsInf(b.AdjFactor, 0) {
			return nil, false
		}
		scale := b.AdjFactor / anchor
		b.Open *= scale
		b.High *= scale
		b.Low *= scale
		b.Close *= scale
		for _, price := range []float64{b.Open, b.High, b.Low, b.Close} {
			if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
				return nil, false
			}
		}
		b.AdjFactor = anchor
		out[date] = b
	}
	return out, true
}
