// 成交流水和独立账本核验，不参与交易决策。
package strategy

import (
	"fmt"
	"math"
	"time"
)

// TradeFill 保存实际参考成交及成交后的账户现金。
type TradeFill struct {
	Date       string  `json:"date"`        // 成交日。
	SignalDate string  `json:"signal_date"` // 首次信号日。
	Side       string  `json:"side"`        // 买卖方向。
	TsCode     string  `json:"ts_code"`     // 证券代码。
	Shares     int     `json:"shares"`      // 成交数量。
	Price      float64 `json:"price"`       // 含滑点成交价。
	Fee        float64 `json:"fee"`         // 实际扣收费用。
	Reason     string  `json:"reason"`      // 卖出主原因，买入为空。
	CashAfter  float64 `json:"cash_after"`  // 成交后现金。
}

// recordFill 在账本实际成交后保存流水，不记录未成交意图。
func (l *Ledger) recordFill(date, signal, side, code string, shares int, price, fee float64, reason string) {
	l.Fills = append(l.Fills, TradeFill{Date: date, SignalDate: signal, Side: side, TsCode: code,
		Shares: shares, Price: price, Fee: fee, Reason: reason, CashAfter: l.Cash})
}

// AuditFills 从成交序列与公司行动重建现金、费用及股数，检查日期、T+1和账户约束。
func (l *Ledger) AuditFills(a AccountConfig) error {
	cash, fees := a.InitialCapital, 0.0
	held, bought := map[string]int{}, map[string]int{}
	lastDate := ""
	type auditEvt struct {
		date string
		kind int // 0: corporate action, 1: trade fill
		fill *TradeFill
		ca   *CorporateActionFill
	}
	evts := make([]auditEvt, 0, len(l.Fills)+len(l.CorporateActions))
	for i := range l.CorporateActions {
		evts = append(evts, auditEvt{date: l.CorporateActions[i].Date, kind: 0, ca: &l.CorporateActions[i]})
	}
	for i := range l.Fills {
		evts = append(evts, auditEvt{date: l.Fills[i].Date, kind: 1, fill: &l.Fills[i]})
	}
	for i := 0; i < len(evts)-1; i++ {
		for j := i + 1; j < len(evts); j++ {
			if evts[j].date < evts[i].date || (evts[j].date == evts[i].date && evts[j].kind < evts[i].kind) {
				evts[i], evts[j] = evts[j], evts[i]
			}
		}
	}
	for i, ev := range evts {
		if ev.kind == 0 {
			ca := ev.ca
			held[ca.TsCode] += ca.BonusShares
			cash += ca.CashDiv
			continue
		}
		f := ev.fill
		fail := func() error { return fmt.Errorf("成交对账失败: 第%d笔 %s %s", i+1, f.Date, f.TsCode) }
		if _, err := time.Parse("2006-01-02", f.Date); err != nil {
			return fail()
		}
		if _, err := time.Parse("2006-01-02", f.SignalDate); err != nil {
			return fail()
		}
		if f.Date < lastDate || f.SignalDate >= f.Date || f.TsCode == "" || f.Shares <= 0 ||
			f.Price <= 0 || f.Fee < 0 {
			return fail()
		}
		for _, v := range []float64{f.Price, f.Fee, f.CashAfter} {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return fail()
			}
		}
		if f.Date != lastDate {
			bought = map[string]int{}
		}
		lastDate = f.Date
		amount := float64(f.Shares) * f.Price
		switch f.Side {
		case "buy":
			held[f.TsCode] += f.Shares
			bought[f.TsCode] += f.Shares
			cash -= amount + f.Fee
		case "sell":
			if f.Shares > held[f.TsCode]-bought[f.TsCode] {
				return fail()
			}
			held[f.TsCode] -= f.Shares
			if held[f.TsCode] == 0 {
				delete(held, f.TsCode)
			}
			cash += amount - f.Fee
		default:
			return fail()
		}
		fees += f.Fee
		if cash < -0.0001 || len(held) > a.MaxPositions || math.Abs(cash-f.CashAfter) > 0.0001 {
			return fail()
		}
	}
	if math.IsNaN(l.Cash) || math.IsInf(l.Cash, 0) || math.IsNaN(l.FeesPaid) || math.IsInf(l.FeesPaid, 0) ||
		math.Abs(cash-l.Cash) > 0.0001 || math.Abs(fees-l.FeesPaid) > 0.0001 || len(held) != len(l.Holdings) {
		return fmt.Errorf("期末现金、费用或持股数量对账失败")
	}
	for code, shares := range held {
		if h := l.Holdings[code]; h == nil || h.Shares != shares {
			return fmt.Errorf("%s 期末持股对账失败", code)
		}
	}
	return nil
}
