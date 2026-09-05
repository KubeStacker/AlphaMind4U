// S0 基线：成交额优先最多两只月度组合，对齐 validation.md S0 定义。
package strategy

import (
	"math"
	"sort"
)

// S0Quote 单日执行报价：开盘参考与收盘估值，停牌/涨跌停由调用方标记。
type S0Quote struct {
	Open      float64 // 次日开盘参考（原价）。
	Close     float64 // 当日收盘（原价估值）。
	AdjFactor float64 // 当日复权因子，用于公司行动核验。
	Suspended bool    // 停牌或无执行价。
	AtLimit   bool    // 开盘涨跌停端点。
}

// S0Input S0 回测输入：月份池、资格、日行情与交易日序列。
type S0Input struct {
	DataQuality DataQuality                 // 冻结数据策略与缺口。
	Checkpoint  func(done, total int) error `json:"-"` // 任务进度与取消检查，不进入输入快照。
	StartDate   string                      // 账户起点；此前日期仅推进月初窗口，不交易或记绩效。
	Market      map[string]MarketInput      // 恢复回撤状态所需市场输入；缺失不允许解锁。
	Threshold   MarketThresholds            // 市场分类阈值。
	Account     AccountConfig               // 账户情景。
	Params      StrategyParams              // 费用与滑点。
	Pools       map[string]PoolVersion      // 生效月 YYYY-MM→股票池。
	Quals       map[string]map[string]Qualify
	Prices           map[string]map[string]S0Quote
	Bars             map[string]map[string]Bar             `json:"bars,omitempty"`
	CorporateActions map[string]map[string]CorporateAction `json:"corporate_actions,omitempty"`
	Dates            []string                              // 升序交易日。
	Lot              func(tsCode string) int               `json:"-"`
	StopPct          func(tsCode, date string) float64     `json:"-"`
}

// S0Point 每日净值点：净值、回撤、持仓数与现金占比诊断。
type S0Point struct {
	Date     string  // 交易日。
	NAV      float64 // 完整账户净资产。
	Drawdown float64 // 相对历史最高回撤。
	Held     int     // 持仓只数。
	CashRate float64 // 现金占比。
}

// S0Result S0 回测结果：账本、净值序列、漏斗、统计与未执行意图。
type S0Result struct {
	Err         error     // 回放阻断原因；失败时其余结果为空，不保留部分绩效。
	Ledger      *Ledger   // 期末账本。
	Series      []S0Point // 逐日净值。
	Funnel      Funnel    // 机会漏斗。
	Trades      int       // 完成买入笔数。
	Halted      bool      // 是否触发 20% 停用。
	OpenIntents []Intent  // 区间末仍未执行的冻结意图，可逐笔对账。
}

// SelectS0 月初按原始 Top20 成交额顺序取前两只通过资格者，不足留现金。
func SelectS0(pool PoolVersion, quals map[string]Qualify, maxPositions int) []PoolEntry {
	out := make([]PoolEntry, 0, maxPositions)
	for _, e := range pool.Entries {
		if len(out) >= maxPositions {
			break
		}
		if q, ok := quals[e.TsCode]; ok && !q.Allowed {
			continue
		}
		out = append(out, e)
	}
	return out
}

// DefaultLot 默认交易单位：688 开头 200 股，其余 100 股。
func DefaultLot(tsCode string) int {
	if len(tsCode) >= 3 && tsCode[:3] == "688" {
		return 200
	}
	return 100
}

// monthOf 取 YYYY-MM。
func monthOf(date string) string {
	if len(date) >= 7 {
		return date[:7]
	}
	return ""
}

// RunS0Backtest S0 月度重演：昨日意图→今日开盘执行→今日收盘估值→冻结明日意图。
//
// 冻结窗口为新月前 4 个交易日（执行落第 5 日内）；仅现金/名额等待可次日重试，
// 其余取消不重试；执行现金校验排除当日卖出释放部分，买单执行日取消即过期。
func RunS0Backtest(in S0Input) S0Result {
	if err := ValidateAccount(in.Account); err != nil {
		return S0Result{Err: err}
	}
	lot := in.Lot
	if lot == nil {
		lot = DefaultLot
	}
	stopPct := in.StopPct
	led := NewLedger(in.Account.InitialCapital)
	res := S0Result{Ledger: led}
	if len(in.Dates) == 0 {
		return res
	}
	if len(in.Pools) > 0 {
		res.Funnel.Raw20 = 20 * len(in.Pools)
	}
	var picks []PoolEntry
	monthDayIdx := 0
	prevMonth := ""
	finished := map[string]bool{} // 当月已成交、沿用或非现金原因取消的候选不再重试。
	var pbuys []pendingBuy
	psells := map[string]pendingSell{}
	closes := map[string]float64{}
	actions := newActionGuard()
	entryATR := map[string]float64{}
	belowMA := map[string]int{}
	coolUntil := map[string]int{}
	var drawdown drawdownTracker
	codes := make([]string, 0, len(in.Prices))
	for code := range in.Prices {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	for i, date := range in.Dates {
		if in.Checkpoint != nil {
			if err := in.Checkpoint(i, len(in.Dates)); err != nil {
				return S0Result{Err: err}
			}
		}
		for _, code := range codes {
			if q, ok := in.Prices[code][date]; ok {
				changed, oldFactor, err := actions.observe(code, date, q.AdjFactor)
				if err != nil {
					return S0Result{Err: err}
				}
				if changed {
					pbuys, _ = CancelPendingBuysForCode(pbuys, code)
					if _, held := led.Holdings[code]; held {
						var act CorporateAction
						if in.CorporateActions != nil && in.CorporateActions[code] != nil {
							act = in.CorporateActions[code][date]
						}
						if err := ApplyCorporateAction(led, code, date, oldFactor, q.AdjFactor, act, entryATR); err != nil {
							return S0Result{Err: err}
						}
					}
				}
			} else if in.DataQuality.Policy == "available_candidates" && actions.bought[code] {
				return S0Result{Err: errInvalid(code + " " + date + ": 曾买入证券缺关键行情，权益或估值不可确认")}
			}
		}
		m := monthOf(date)
		if m != prevMonth {
			prevMonth, monthDayIdx = m, 0
			picks = nil
			finished = map[string]bool{}
			for code := range led.Holdings {
				finished[code] = true
			}
			if pool, ok := in.Pools[m]; ok {
				picks = SelectS0(pool, in.Quals[m], in.Account.MaxPositions)
				res.Funnel.Tradable += len(picks)
				res.Funnel.SignalHit += len(picks)
			}
		} else {
			monthDayIdx++
		}
		if in.StartDate != "" && date < in.StartDate {
			continue
		}
		inPool := map[string]bool{}
		for _, e := range picks {
			inPool[e.TsCode] = true
		}
		cashAtOpen := math.Max(0, led.Cash-in.Account.CashReserve)
		openPrices := make(map[string]float64, len(closes))
		for code, price := range closes {
			openPrices[code] = price
		}
		for code, quotes := range in.Prices {
			if q, ok := quotes[date]; ok && !q.Suspended && q.Open > 0 {
				openPrices[code] = q.Open
			}
		}
		// 1. 执行昨日卖出意图：受阻保留原由，不删仓。
		for _, code := range sortedKeys(psells) {
			s := psells[code]
			h := led.Holdings[code]
			if h == nil {
				delete(psells, code)
				continue
			}
			q, ok := in.Prices[code][date]
			if !ok || q.Suspended || q.AtLimit {
				continue
			}
			px := SellFillPrice(q.Open, in.Params)
			if px <= 0 {
				continue
			}
			sh := s.Shares
			if sh > h.Shares {
				sh = h.Shares
			}
			fee := CommissionFee(float64(sh)*px, in.Params) + float64(sh)*px*in.Params.StampRate
			led.ApplySell(code, sh, px, fee)
			led.recordFill(date, s.SignalDate, "sell", code, sh, px, fee, string(s.Reason))
			if led.Holdings[code] == nil {
				coolUntil[code] = i
			}
			delete(psells, code)
		}
		// 2. 执行昨日买入意图：只用昨日已可用现金，不含今日释放部分。
		spentToday := 0.0
		for _, b := range pbuys {
			use, why := true, ""
			if _, held := led.Holdings[b.TsCode]; held {
				use, why = false, "held"
			} else if len(led.Holdings) >= in.Account.MaxPositions {
				use, why = false, "nocash"
			}
			q, ok := in.Prices[b.TsCode][date]
			px := 0.0
			if use {
				if !ok || q.Suspended || q.AtLimit || q.Open <= 0 {
					use, why = false, "rejected"
				} else {
					px = BuyFillPrice(q.Open, in.Params)
					if b.SigPx > 0 && q.Open/b.SigPx-1 > in.Params.GapUpLimit {
						use, why = false, "rejected"
					} else if !(px > b.Stop) || px <= 0 {
						use, why = false, "rejected"
					}
				}
			}
			fee := 0.0
			if use && !buyBudgetAllowed(led, openPrices, b.TsCode, b.Shares, px, b.Stop,
				b.MarketCap, in.Account, in.Params) {
				use, why = false, "rejected"
			}
			if use {
				fee = CommissionFee(float64(b.Shares)*px, in.Params)
				if float64(b.Shares)*px+fee+spentToday > cashAtOpen {
					use, why = false, "nocash"
				}
			}
			if !use {
				if why == "nocash" {
					res.Funnel.NoCash++
				} else {
					res.Funnel.Rejected++
					finished[b.TsCode] = true
				}
				continue
			}
			led.ApplyBuy(b.TsCode, b.Shares, px, fee, b.Stop)
			led.recordFill(date, b.SignalDate, "buy", b.TsCode, b.Shares, px, fee, "")
			actions.bought[b.TsCode] = true
			entryATR[b.TsCode] = b.ATR
			finished[b.TsCode] = true
			spentToday += float64(b.Shares)*px + fee
			res.Funnel.Filled++
			res.Trades++
		}
		pbuys = nil
		// 3. 今日收盘估值。
		for code, pm := range in.Prices {
			if q, ok := pm[date]; ok && !q.Suspended && q.Close > 0 {
				closes[code] = q.Close
			}
		}
		nav := led.NAV(closes)
		led.MarkPeak(nav)
		cover := drawdown.update(nav, led.PeakNAV, in.Account, recoveryMarketOK(in.Market, date, in.Threshold))
		if !res.Halted && cover.Halt {
			res.Halted = true
		}
		// 4a. 跟踪退出线只升不降。
		for code, h := range led.Holdings {
			px, ok := closes[code]
			if !ok {
				continue
			}
			if px > h.High {
				h.High = px
			}
			if atr, has := entryATR[code]; has && atr > 0 {
				if trail := h.High - in.Params.TrailAtr*atr; trail > h.Stop {
					h.Stop = trail
				}
			}
		}
		// 4b. 冻结明日卖出意图：停用/离池/跌破/均线任一，保留最高优先级理由。
		for _, code := range sortedHoldings(led) {
			h := led.Holdings[code]
			offPool := !inPool[code]
			px, ok := closes[code]
			belowStop := false
			if ok && h.Stop > 0 && px < h.Stop {
				belowStop = true
			}
			maDays := belowMA[code]
			if in.Bars != nil && in.Bars[code] != nil && ok {
				if closesList, okCloses := windowCloses(in.Bars[code], in.Dates, i, in.Params.MaFast); okCloses {
					ma := SMA(closesList, in.Params.MaFast)
					if ma.Available() {
						if px < ma.Value {
							maDays++
						} else {
							maDays = 0
						}
						belowMA[code] = maDays
					}
				}
			}
			reason := ExitSignal(ExitInput{OffPool: offPool, BelowStop: belowStop,
				BelowMaDays: maDays, MaExitDays: in.Params.MaExitDays,
				DataRisk: false, AccountHalt: res.Halted,
				MarketCut: false})
			if reason == ExitNone {
				continue
			}
			psells[code] = mergePendingSell(psells[code], pendingSell{TsCode: code, Shares: h.Shares,
				Reason: reason, SignalDate: date})
		}
		if err := queueReductions(led, closes, in.Account, cover.TargetCap(0.80), date, lot, psells); err != nil {
			return S0Result{Err: err}
		}
		if err := queueRiskReductions(led, closes, in.Account, in.Params, date, lot, psells); err != nil {
			return S0Result{Err: err}
		}
		// 4c. 冻结明日买入意图：仅现金/名额等待可次日重试。
		if !res.Halted && !cover.FreezeNew && len(picks) > 0 && monthDayIdx < 4 {
			free := in.Account.MaxPositions - len(led.Holdings)
			if free > 0 {
				nav2 := led.NAV(closes)
				_, avail := TargetBudget(BudgetInput{NAV: nav2, Cash: led.Cash,
					HeldValue: nav2 - led.Cash, MarketCap: cover.TargetCap(0.80),
					Account: in.Account})
				each, reserved, frozen := avail/float64(free), 0.0, 0
				riskRoom := remainingRisk(led, closes, nav2, in.Account, in.Params)
				for _, e := range picks {
					if frozen >= free {
						break
					}
					if _, held := led.Holdings[e.TsCode]; held {
						continue
					}
					if ci, ok := coolUntil[e.TsCode]; ok && i-ci <= in.Params.CooldownDays {
						continue
					}
					if finished[e.TsCode] {
						continue
					}
					q, ok := in.Prices[e.TsCode][date]
					if !ok || q.Close <= 0 {
						res.Funnel.Rejected++
						finished[e.TsCode] = true
						continue
					}
					atrVal := 0.0
					sd := 0.0
					if in.Bars != nil && in.Bars[e.TsCode] != nil {
						if bList, ok := windowBars(in.Bars[e.TsCode], in.Dates, i, in.Params.AtrN+1); ok {
							atr := ATR20(bList, in.Params.AtrN)
							if atr.Available() && atr.Value > 0 && q.Close > 0 {
								atrVal = atr.Value
								sd = in.Params.InitStopAtr * atrVal / q.Close
							}
						}
					}
					if sd <= 0 && stopPct != nil {
						sd = stopPct(e.TsCode, date)
					}
					if sd <= 0 {
						sd = 0.08
					}
					if sd <= 0 {
						res.Funnel.Rejected++
						finished[e.TsCode] = true
						continue
					}
					worstPx := BuyFillPrice(q.Close*(1+in.Params.GapUpLimit), in.Params)
					sh, reason := SizeShares(nav2, math.Min(each, avail-reserved),
						sd, worstPx, in.Account, lot(e.TsCode))
					if reason != "" || sh <= 0 {
						res.Funnel.NoCash++
						continue
					}
					stop := q.Close - in.Params.InitStopAtr*atrVal
					if atrVal == 0 {
						stop = q.Close - q.Close*sd
					}
					sh, risk, est := sizeRiskBuy(sh, lot(e.TsCode), worstPx,
						stop, nav2, math.Min(each, avail-reserved), riskRoom, in.Account, in.Params)
					if sh <= 0 {
						res.Funnel.NoCash++
						continue
					}
					pbuys = append(pbuys, pendingBuy{TsCode: e.TsCode, Shares: sh,
						SigPx: q.Close, Stop: stop, ATR: atrVal, SignalDate: date, MarketCap: cover.TargetCap(0.80)})
					reserved += est
					riskRoom -= risk
					frozen++
				}
			}
		}
		dd := 0.0
		if led.PeakNAV > 0 {
			dd = 1 - nav/led.PeakNAV
		}
		cashRate := 1.0
		if nav > 0 {
			cashRate = led.Cash / nav
		}
		res.Series = append(res.Series, S0Point{Date: date, NAV: nav, Drawdown: dd,
			Held: len(led.Holdings), CashRate: cashRate})
	}
	for _, b := range pbuys {
		res.OpenIntents = append(res.OpenIntents, Intent{SignalDate: b.SignalDate,
			Side: "buy", TsCode: b.TsCode, Shares: b.Shares, RefPrice: b.SigPx, StopPrice: b.Stop, MarketCap: b.MarketCap})
	}
	for _, code := range sortedKeys(psells) {
		s := psells[code]
		res.OpenIntents = append(res.OpenIntents, Intent{SignalDate: s.SignalDate,
			Side: "sell", TsCode: s.TsCode, Shares: s.Shares, Reason: s.Reason})
	}
	if err := led.AuditFills(in.Account); err != nil {
		return S0Result{Err: err}
	}
	return res
}

// sortedKeys 待卖意图代码升序，保证执行顺序可重演。
func sortedKeys(m map[string]pendingSell) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sortedHoldings 持仓代码升序，保证意图冻结顺序可重演。
func sortedHoldings(l *Ledger) []string {
	out := make([]string, 0, len(l.Holdings))
	for k := range l.Holdings {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
