// S1/S2 回放：趋势筛选与指数择时逐日重演，对齐 signals.md 与 execution.md。
//
// 近似等级（日线近似，不作实盘执行认证）：信号日盘后冻结意图、次日开盘参考执行；
// 行业/相关性 50% 上限、E1/E2/E3 分支后置，只做 S1/S2/F1 主干；分钟级同日换仓另开版本。
package strategy

import (
	"math"
	"sort"
)

// S12Version 策略版本：S1 固定 80% 上限，S2 加指数择时，F1 为 S2 改每日评估。
type S12Version string

// 策略版本常量。
const (
	VerS1 S12Version = "S1" // 趋势筛选，固定 80% 上限。
	VerS2 S12Version = "S2" // S1 加指数趋势与宽度择时。
	VerF1 S12Version = "F1" // S2 仅改每日入场评估。
)

// S12Input S1/S2 回放输入：股票池、日线、指数、市场状态与交易日序列。
type S12Input struct {
	DataQuality    DataQuality                 // 冻结数据策略与缺口。
	DecisionMonths map[string]string           // 决策日→下一交易日月份，用于月末换池。
	Checkpoint     func(done, total int) error `json:"-"` // 任务进度与取消检查，不进入输入快照。
	Account        AccountConfig               // 账户情景。
	Params         StrategyParams              // 策略与费用参数。
	Threshold      MarketThresholds            // 市场状态阈值。
	Version        S12Version                  // S1 / S2 / F1。
	Pools          map[string]PoolVersion      // 生效月→股票池。
	Quals          map[string]map[string]Qualify
	Bars           map[string]map[string]Bar // 代码→日期→原始Bar及因子；指标另取信号日尺度副本。
	Index          map[string]float64        // 日期→沪深300收盘。
	Market         map[string]MarketInput    // 日期→市场状态输入。
	Dates          []string                  // 升序交易日。
	StartDate      string                    // 账户起点；此前Dates只供预热，空值从首日开始。
	EntryDays      map[string]bool           // 正式日历判定的周/月末评估日。
	Suspend          map[string]map[string]bool
	LimitDay         map[string]map[string]bool
	CorporateActions map[string]map[string]CorporateAction `json:"corporate_actions,omitempty"`
	Lot              func(tsCode string) int               `json:"-"`
}

// S12Result S1/S2 回放结果。
type S12Result struct {
	Err         error     // 回放阻断原因；失败时其余结果为空，不保留部分绩效。
	Ledger      *Ledger   // 期末账本。
	Series      []S0Point // 逐日净值（复用 S0 点结构）。
	Funnel      Funnel    // 机会与资金漏斗。
	Trades      int       // 完成买入笔数。
	Halted      bool      // 是否触发 20% 停用。
	OpenIntents []Intent  // 区间末仍未执行的冻结意图，可逐笔对账。
}

// pendingBuy 次日执行买单：股数与风控线冻结，只收紧不放宽。
type pendingBuy struct {
	TsCode     string  // 证券代码。
	Shares     int     // 冻结股数。
	SigPx      float64 // 信号收盘。
	Stop       float64 // 初始退出线。
	MA20       float64 // 信号日 MA20。
	ATR        float64 // 入场 ATR（冻结用于跟踪）。
	SignalDate string  // 信号日（冻结日）。
	MarketCap  float64 // 信号日冻结的市场/回撤比例上限。
}

// pendingSell 次日执行卖单：受阻跨日保留原由，成交后清理。
type pendingSell struct {
	TsCode     string     // 证券代码。
	Shares     int        // 目标卖量。
	Reason     ExitReason // 退出原因（合并保留最高优先级）。
	SignalDate string     // 信号日（首次冻结日）。
}

// mergePendingSell 保留首次日期与较大待卖量，只升级更高优先级的退出原因。
func mergePendingSell(old, next pendingSell) pendingSell {
	if old.SignalDate == "" {
		return next
	}
	if next.Shares > old.Shares {
		old.Shares = next.Shares
	}
	if exitPriority(next.Reason) > exitPriority(old.Reason) {
		old.Reason = next.Reason
	}
	return old
}

// entryDay 是否允许新开仓评估：F1每日，S1/S2使用正式日历标记。
func entryDay(ver S12Version, date string, entries map[string]bool) bool {
	return ver == VerF1 || entries[date]
}

// windowCloses 取截至 idx 的最近 n 个收盘，要求日期连续存在，否则缺历史。
func windowCloses(bars map[string]Bar, dates []string, idx, n int) ([]float64, bool) {
	if idx < 0 || idx >= len(dates) || n <= 0 || n > idx+1 {
		return nil, false
	}
	out := make([]float64, 0, n)
	for _, d := range dates[idx-n+1 : idx+1] {
		b, ok := bars[d]
		if !ok || b.Close <= 0 {
			return nil, false
		}
		out = append(out, b.Close)
	}
	return out, true
}

// windowBars 取截至 idx 的最近 n 个 Bar。
func windowBars(bars map[string]Bar, dates []string, idx, n int) ([]Bar, bool) {
	if idx < 0 || idx >= len(dates) || n <= 0 || n > idx+1 {
		return nil, false
	}
	out := make([]Bar, 0, n)
	for _, d := range dates[idx-n+1 : idx+1] {
		b, ok := bars[d]
		if !ok || b.Close <= 0 || b.High <= 0 || b.Low <= 0 {
			return nil, false
		}
		out = append(out, b)
	}
	return out, true
}

// suspended 是否停牌缺执行价。
func (in S12Input) suspended(code, date string) bool {
	if m, ok := in.Suspend[code]; ok && m[date] {
		return true
	}
	if m, ok := in.Bars[code]; ok {
		if _, has := m[date]; !has {
			return true
		}
		return false
	}
	return true
}

// atLimit 是否开盘涨跌停端点。
func (in S12Input) atLimit(code, date string) bool {
	if m, ok := in.LimitDay[code]; ok && m[date] {
		return true
	}
	return false
}

// dailyCap 分别返回存量目标比例、新增许可和市场状态，禁止新买不等于清仓。
func (in S12Input) dailyCap(date string, dd DrawdownState) (float64, bool, MarketState) {
	if in.Version == VerS1 {
		cap := dd.TargetCap(0.80)
		return cap, cap > 0 && !dd.FreezeNew && !dd.Halt, StateOther
	}
	mi, ok := in.Market[date]
	if !ok {
		return 0, false, StateUnavailable
	}
	st := ClassifyState(mi, in.Threshold)
	if st == StateUnavailable {
		return 0, false, st
	}
	cap := dd.TargetCap(StateCap(st, in.Threshold))
	canBuy := cap > 0 && !dd.FreezeNew && !dd.Halt && st != StateHot && st != StateRetreat
	return cap, canBuy, st
}

// RunS12Backtest S1/S2/F1 逐日重演：先执行昨日意图，再估值，最后冻结今日意图。
func RunS12Backtest(in S12Input) S12Result {
	if err := ValidateAccount(in.Account); err != nil {
		return S12Result{Err: err}
	}
	lot := in.Lot
	if lot == nil {
		lot = DefaultLot
	}
	led := NewLedger(in.Account.InitialCapital)
	res := S12Result{Ledger: led}
	if len(in.Dates) == 0 {
		return res
	}
	if len(in.Pools) > 0 {
		res.Funnel.Raw20 = 20 * len(in.Pools)
	}
	var buys []pendingBuy
	psells := map[string]pendingSell{}
	closes := map[string]float64{}
	belowMA := map[string]int{}
	coolUntil := map[string]int{}
	entryATR := map[string]float64{}
	curPool := map[string]bool{}
	curMonth := ""
	actions := newActionGuard()
	var drawdown drawdownTracker
	codes := make([]string, 0, len(in.Bars))
	for code := range in.Bars {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	for i, date := range in.Dates {
		if in.Checkpoint != nil {
			if err := in.Checkpoint(i, len(in.Dates)); err != nil {
				return S12Result{Err: err}
			}
		}
		for _, code := range codes {
			if b, ok := in.Bars[code][date]; ok {
				changed, oldFactor, err := actions.observe(code, date, b.AdjFactor)
				if err != nil {
					return S12Result{Err: err}
				}
				if changed {
					buys, _ = CancelPendingBuysForCode(buys, code)
					if _, held := led.Holdings[code]; held {
						var act CorporateAction
						if in.CorporateActions != nil && in.CorporateActions[code] != nil {
							act = in.CorporateActions[code][date]
						}
						if err := ApplyCorporateAction(led, code, date, oldFactor, b.AdjFactor, act, entryATR); err != nil {
							return S12Result{Err: err}
						}
					}
				}
			} else if in.DataQuality.Policy == "available_candidates" && actions.bought[code] {
				return S12Result{Err: errInvalid(code + " " + date + ": 曾买入证券缺关键行情，权益或估值不可确认")}
			}
		}
		if in.StartDate != "" && date < in.StartDate {
			continue
		}
		m, known := in.DecisionMonths[date]
		if !known {
			return S12Result{Err: errInvalid("缺少决策日的后继月份: " + date)}
		}
		if _, ok := in.Pools[m]; !ok {
			return S12Result{Err: errInvalid("缺少决策月池: " + m)}
		}
		if m != curMonth {
			curMonth = m
			curPool = map[string]bool{}
			if pool, ok := in.Pools[m]; ok {
				for _, e := range pool.Entries {
					curPool[e.TsCode] = true
				}
			}
		}
		// 1. 执行昨日意图中的卖单：受阻跨日保留原由，成交后才清理。
		cashAtOpen := math.Max(0, led.Cash-in.Account.CashReserve)
		openPrices := make(map[string]float64, len(closes))
		for code, price := range closes {
			openPrices[code] = price
		}
		for code, bars := range in.Bars {
			if b, ok := bars[date]; ok && !in.suspended(code, date) && b.Open > 0 {
				openPrices[code] = b.Open
			}
		}
		for _, code := range sortedKeys(psells) {
			s := psells[code]
			h := led.Holdings[code]
			if h == nil {
				delete(psells, code)
				continue
			}
			if in.suspended(code, date) {
				continue // 被困保留账面与原由。
			}
			b, ok := in.Bars[code][date]
			if !ok || b.Open <= 0 {
				continue
			}
			if in.atLimit(code, date) {
				continue // 端点不可成交；停用亦不豁免，意图保留。
			}
			px := SellFillPrice(b.Open, in.Params)
			sh := s.Shares
			if sh > h.Shares {
				sh = h.Shares
			}
			fee := CommissionFee(float64(sh)*px, in.Params) + float64(sh)*px*in.Params.StampRate
			led.ApplySell(code, sh, px, fee)
			led.recordFill(date, s.SignalDate, "sell", code, sh, px, fee, string(s.Reason))
			delete(psells, code)
			if _, held := led.Holdings[code]; !held {
				delete(entryATR, code)
				coolUntil[code] = i
				belowMA[code] = 0
			}
		}
		// 2. 执行昨日意图中的买单：只用昨日已可用现金，不含今日释放部分。
		spentToday := 0.0
		for _, b := range buys {
			if _, held := led.Holdings[b.TsCode]; held {
				res.Funnel.Rejected++
				continue
			}
			if len(led.Holdings) >= in.Account.MaxPositions {
				res.Funnel.NoCash++
				continue
			}
			if in.suspended(b.TsCode, date) || in.atLimit(b.TsCode, date) {
				res.Funnel.Rejected++
				continue
			}
			bar, ok := in.Bars[b.TsCode][date]
			if !ok || bar.Open <= 0 {
				res.Funnel.Rejected++
				continue
			}
			if gate := CheckBuyGate(b.SigPx, bar.Open, BuyFillPrice(bar.Open, in.Params),
				b.Stop, b.MA20, b.ATR, false, in.Params); gate != "" {
				res.Funnel.Rejected++
				continue
			}
			px := BuyFillPrice(bar.Open, in.Params)
			if !buyBudgetAllowed(led, openPrices, b.TsCode, b.Shares, px, b.Stop,
				b.MarketCap, in.Account, in.Params) {
				res.Funnel.Rejected++
				continue
			}
			fee := CommissionFee(float64(b.Shares)*px, in.Params)
			if float64(b.Shares)*px+fee+spentToday > cashAtOpen {
				res.Funnel.NoCash++
				continue
			}
			led.ApplyBuy(b.TsCode, b.Shares, px, fee, b.Stop)
			led.recordFill(date, b.SignalDate, "buy", b.TsCode, b.Shares, px, fee, "")
			actions.bought[b.TsCode] = true
			entryATR[b.TsCode] = b.ATR
			spentToday += float64(b.Shares)*px + fee
			res.Funnel.Filled++
			res.Trades++
		}
		buys = buys[:0]
		// 3. 收盘估值与回撤。
		for code, pm := range in.Bars {
			if b, ok := pm[date]; ok && b.Close > 0 {
				closes[code] = b.Close
			}
		}
		nav := led.NAV(closes)
		led.MarkPeak(nav)
		cover := drawdown.update(nav, led.PeakNAV, in.Account, recoveryMarketOK(in.Market, date, in.Threshold))
		if cover.Halt {
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
		// 4b. 冻结今日卖出意图：买入日收盘亦可形成次日意图（执行恒在次日，满足 T+1）。
		capNow, canBuy, marketState := in.dailyCap(date, cover)
		for code, h := range led.Holdings {
			offPool := !curPool[code]
			belowStop := false
			if px, ok := closes[code]; ok && h.Stop > 0 && px < h.Stop {
				belowStop = true
			}
			maDays := belowMA[code]
			if snap, ok := daySnapshot(in, code, i); ok && snap.MA20.Available() {
				if snap.Close < snap.MA20.Value {
					maDays++
				} else {
					maDays = 0
				}
				belowMA[code] = maDays
			}
			reason := ExitSignal(ExitInput{OffPool: offPool, BelowStop: belowStop,
				BelowMaDays: maDays, MaExitDays: in.Params.MaExitDays,
				DataRisk: false, AccountHalt: res.Halted,
				MarketCut: marketState != StateUnavailable && capNow == 0})
			if reason == ExitNone {
				continue
			}
			psells[code] = mergePendingSell(psells[code], pendingSell{TsCode: code, Shares: h.Shares,
				Reason: reason, SignalDate: date})
		}
		if marketState != StateUnavailable {
			if err := queueReductions(led, closes, in.Account, capNow, date, lot, psells); err != nil {
				return S12Result{Err: err}
			}
		}
		if err := queueRiskReductions(led, closes, in.Account, in.Params, date, lot, psells); err != nil {
			return S12Result{Err: err}
		}
		// 4c. 冻结今日买入意图（入场评估日、冷却已过、预算充足）。
		if !res.Halted && entryDay(in.Version, date, in.EntryDays) {
			if canBuy {
				cands := scanCandidates(in, date, i, curMonth)
				res.Funnel.Tradable += len(cands)
				_, avail := TargetBudget(BudgetInput{NAV: nav, Cash: led.Cash,
					HeldValue: nav - led.Cash, MarketCap: capNow, Account: in.Account})
				free := in.Account.MaxPositions - len(led.Holdings) - len(buys)
				if free > 0 && avail > 0 {
					each := avail / float64(free)
					reserved := 0.0
					riskRoom := remainingRisk(led, closes, nav, in.Account, in.Params)
					n := 0
					for _, c := range cands {
						if n >= free {
							break
						}
						if _, held := led.Holdings[c.code]; held {
							continue
						}
						if ci, ok := coolUntil[c.code]; ok && i-ci <= in.Params.CooldownDays {
							continue
						}
						worstPx := BuyFillPrice(c.close*(1+in.Params.GapUpLimit), in.Params)
						stop := c.close - in.Params.InitStopAtr*c.atr
						sh, reason := SizeShares(nav, each, in.Params.InitStopAtr*c.atr/c.close,
							worstPx, in.Account, lot(c.code))
						if reason != "" || sh <= 0 {
							res.Funnel.NoCash++
							continue
						}
						sh, risk, cost := sizeRiskBuy(sh, lot(c.code), worstPx,
							stop, nav, math.Min(each, avail-reserved), riskRoom, in.Account, in.Params)
						if sh <= 0 {
							res.Funnel.NoCash++
							continue
						}
						buys = append(buys, pendingBuy{TsCode: c.code, Shares: sh,
							SigPx: c.close, Stop: stop, MA20: c.ma20, ATR: c.atr, SignalDate: date, MarketCap: capNow})
						riskRoom -= risk
						reserved += cost
						n++
					}
					res.Funnel.SignalHit += n
					res.Funnel.MarketPass += n
				}
			}
		}
		nav = led.NAV(closes)
		led.MarkPeak(nav)
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
	for _, b := range buys {
		res.OpenIntents = append(res.OpenIntents, Intent{SignalDate: b.SignalDate,
			Side: "buy", TsCode: b.TsCode, Shares: b.Shares, RefPrice: b.SigPx, StopPrice: b.Stop, MarketCap: b.MarketCap})
	}
	for _, code := range sortedKeys(psells) {
		s := psells[code]
		res.OpenIntents = append(res.OpenIntents, Intent{SignalDate: s.SignalDate,
			Side: "sell", TsCode: s.TsCode, Shares: s.Shares, Reason: s.Reason})
	}
	if err := led.AuditFills(in.Account); err != nil {
		return S12Result{Err: err}
	}
	return res
}

// candidate 候选：排序键与冻结价。
type candidate struct {
	code  string  // 证券代码。
	rs    float64 // RS20。
	rank  int     // 月金额名次。
	close float64 // 信号收盘。
	ma20  float64 // MA20。
	atr   float64 // ATR20。
}

// scanCandidates 扫描当日 Top20 内触发入场的候选，按 RS/名次/代码排序。
func scanCandidates(in S12Input, date string, idx int, month string) []candidate {
	pool, ok := in.Pools[month]
	if !ok {
		return nil
	}
	rank := map[string]int{}
	for _, e := range pool.Entries {
		rank[e.TsCode] = e.Rank
	}
	out := []candidate{}
	for _, e := range pool.Entries {
		code := e.TsCode
		if q, has := in.Quals[month]; has {
			if qq, has2 := q[code]; has2 && !qq.Allowed {
				continue
			}
		}
		if in.suspended(code, date) {
			continue
		}
		snap, ok := daySnapshot(in, code, idx)
		if !ok {
			continue
		}
		ent := BuildEntry(snap, true, true, true, in.Params)
		if !EntrySignal(ent, in.Params) {
			continue
		}
		out = append(out, candidate{code: code, rs: snap.RS.Value, rank: rank[code],
			close: snap.Close, ma20: snap.MA20.Value, atr: snap.ATR.Value})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].rs != out[j].rs {
			return out[i].rs > out[j].rs
		}
		if out[i].rank != out[j].rank {
			return out[i].rank < out[j].rank
		}
		return out[i].code < out[j].code
	})
	return out
}

// daySnapshot 组装单股单日趋势快照：窗口不足任一即返回不可用。
func daySnapshot(in S12Input, code string, idx int) (TrendSnapshot, bool) {
	var s TrendSnapshot
	series, ok := in.Bars[code]
	if !ok {
		return s, false
	}
	dates := in.Dates
	p := in.Params
	if idx < 0 || idx >= len(dates) {
		return s, false
	}
	for _, period := range []int{p.MaSlow, p.MaFast, p.SlopeN, p.MomentumN, p.AtrN, p.BreakoutN, p.AmountN} {
		if period <= 0 || period > idx+1 {
			return s, false
		}
	}
	closeN := max(p.MaSlow, p.MaFast+p.SlopeN, p.MomentumN+1)
	n := max(closeN, p.AtrN+1, p.BreakoutN+1, p.AmountN+1)
	series, ok = signalBars(series, dates, idx, n)
	if !ok {
		return s, false
	}
	closes, ok := windowCloses(series, dates, idx, closeN)
	if !ok {
		return s, false
	}
	s.Close = closes[len(closes)-1]
	s.MA20 = SMA(closes, in.Params.MaFast)
	s.MA60 = SMA(closes, in.Params.MaSlow)
	prev := closes[:len(closes)-in.Params.SlopeN]
	s.MA20Prev = SMA(prev, in.Params.MaFast)
	s.RS = func() FactorResult {
		idxCloses, ok := windowIndexCloses(in.Index, dates, idx, in.Params.MomentumN+1)
		if !ok {
			return bad("指数窗口不足")
		}
		return RS20(closes, idxCloses, in.Params.MomentumN)
	}()
	bars, ok := windowBars(series, dates, idx, in.Params.AtrN+1)
	if !ok {
		return s, false
	}
	s.ATR = ATR20(bars, in.Params.AtrN)
	preCloses, ok := windowCloses(series, dates, idx-1, in.Params.BreakoutN)
	if !ok {
		return s, false
	}
	s.HC20 = HighestClose(preCloses, in.Params.BreakoutN)
	amounts := make([]float64, 0, in.Params.AmountN+1)
	if idx-in.Params.AmountN < 0 {
		return s, false
	}
	for _, d := range dates[idx-in.Params.AmountN : idx+1] {
		b, has := series[d]
		if !has {
			return s, false
		}
		amounts = append(amounts, b.Amount)
	}
	s.AmountR = AmountRatio(amounts, in.Params.AmountN)
	if !s.MA20.Available() || !s.MA60.Available() || !s.RS.Available() ||
		!s.HC20.Available() || !s.AmountR.Available() || !s.ATR.Available() || !s.MA20Prev.Available() {
		return s, false
	}
	return s, true
}

// windowIndexCloses 仅取对应交易日的指数窗口，不复制整段指数历史。
func windowIndexCloses(prices map[string]float64, dates []string, idx, n int) ([]float64, bool) {
	if idx < 0 || idx >= len(dates) || n <= 0 || n > idx+1 {
		return nil, false
	}
	out := make([]float64, 0, n)
	for _, date := range dates[idx-n+1 : idx+1] {
		price, ok := prices[date]
		if !ok || price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
			return nil, false
		}
		out = append(out, price)
	}
	return out, true
}
