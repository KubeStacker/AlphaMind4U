// 持仓周期按完全退出合并，基础统计不参与交易决策。
package strategy

// TradeCycle 描述一次建仓至完全退出，未平仓时不提供净盈亏。
type TradeCycle struct {
	TsCode          string   `json:"ts_code"`          // 证券代码。
	EntryDate       string   `json:"entry_date"`       // 首次建仓日。
	ExitDate        string   `json:"exit_date"`        // 完全退出日，未退出为空。
	SharesRemaining int      `json:"shares_remaining"` // 未退出数量。
	BuyAmount       float64  `json:"buy_amount"`       // 累计买入成交金额。
	SellAmount      float64  `json:"sell_amount"`      // 累计卖出成交金额。
	Fees            float64  `json:"fees"`             // 周期实际费用。
	Dividends       float64  `json:"dividends"`        // 周期现金分红到账。
	NetPnL          *float64 `json:"net_pnl"`          // 已闭合周期净盈亏，未闭合为null。
}

// TradeSummary 汇总模型内闭合周期，不代表样本外或未来胜率。
type TradeSummary struct {
	ClosedCount  int      `json:"closed_count"`   // 完成周期数。
	OpenCount    int      `json:"open_count"`     // 未完成周期数。
	Wins         int      `json:"wins"`           // 净盈利周期。
	Losses       int      `json:"losses"`         // 净亏损周期。
	Flats        int      `json:"flats"`          // 净盈亏为零周期，保留在分母。
	ClosedNetPnL float64  `json:"closed_net_pnl"` // 完成周期净盈亏和。
	WinRate      *float64 `json:"win_rate"`       // 无完成周期为null。
	ProfitFactor *float64 `json:"profit_factor"`  // 无亏损周期为null。
}

// TradeCycles 在流水核验通过后计算周期及基础描述统计，顺序按首次建仓排列。
func (l *Ledger) TradeCycles() ([]TradeCycle, TradeSummary) {
	cycles := []TradeCycle{}
	active := map[string]int{}
	type cycleEvent struct {
		date string
		kind int // 0: ca, 1: fill
		fill *TradeFill
		ca   *CorporateActionFill
	}
	events := make([]cycleEvent, 0, len(l.Fills)+len(l.CorporateActions))
	for i := range l.CorporateActions {
		events = append(events, cycleEvent{date: l.CorporateActions[i].Date, kind: 0, ca: &l.CorporateActions[i]})
	}
	for i := range l.Fills {
		events = append(events, cycleEvent{date: l.Fills[i].Date, kind: 1, fill: &l.Fills[i]})
	}
	// 稳定排序：同一日期公司行动在交易之前生效
	for i := 0; i < len(events)-1; i++ {
		for j := i + 1; j < len(events); j++ {
			if events[j].date < events[i].date || (events[j].date == events[i].date && events[j].kind < events[i].kind) {
				events[i], events[j] = events[j], events[i]
			}
		}
	}
	for _, ev := range events {
		if ev.kind == 0 {
			ca := ev.ca
			if i, exists := active[ca.TsCode]; exists {
				cycles[i].Dividends += ca.CashDiv
				cycles[i].SharesRemaining += ca.BonusShares
			}
			continue
		}
		f := ev.fill
		i, exists := active[f.TsCode]
		if !exists {
			i = len(cycles)
			active[f.TsCode] = i
			cycles = append(cycles, TradeCycle{TsCode: f.TsCode, EntryDate: f.Date})
		}
		c := &cycles[i]
		amount := float64(f.Shares) * f.Price
		c.Fees += f.Fee
		if f.Side == "buy" {
			c.BuyAmount += amount
			c.SharesRemaining += f.Shares
		} else {
			c.SellAmount += amount
			c.SharesRemaining -= f.Shares
		}
		if c.SharesRemaining == 0 {
			c.ExitDate = f.Date
			net := c.SellAmount - c.BuyAmount - c.Fees + c.Dividends
			c.NetPnL = &net
			delete(active, f.TsCode)
		}
	}
	var summary TradeSummary
	profit, loss := 0.0, 0.0
	for _, c := range cycles {
		if c.NetPnL == nil {
			summary.OpenCount++
			continue
		}
		summary.ClosedCount++
		net := *c.NetPnL
		summary.ClosedNetPnL += net
		switch {
		case net > 0:
			summary.Wins++
			profit += net
		case net < 0:
			summary.Losses++
			loss -= net
		default:
			summary.Flats++
		}
	}
	if summary.ClosedCount > 0 {
		rate := float64(summary.Wins) / float64(summary.ClosedCount)
		summary.WinRate = &rate
	}
	if loss > 0 {
		factor := profit / loss
		summary.ProfitFactor = &factor
	}
	return cycles, summary
}
