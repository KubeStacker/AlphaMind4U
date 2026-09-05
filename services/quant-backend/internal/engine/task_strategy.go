// Package engine 策略研究装配：股票池重建，回测仍由 strategy 纯函数承载。
package engine

import (
	"fmt"

	"quantbackend/internal/strategy"
)

// RebuildTop20Pool 重建指定生效月的上月 Top20：排名月为生效月的前一自然月。
func (e *Engine) RebuildTop20Pool(effectMonth string) (strategy.PoolVersion, error) {
	rankMonth := prevMonthOf(effectMonth)
	if rankMonth == "" {
		return strategy.PoolVersion{}, fmt.Errorf("非法月份: %s", effectMonth)
	}
	return strategy.FetchMonthlyTop20(e.DB, effectMonth, rankMonth, datasetVersion())
}

// SnapshotTop20Pool 重建并追加内容版本，不覆盖旧月池。
func (e *Engine) SnapshotTop20Pool(effectMonth string) (strategy.PoolVersion, error) {
	pool, err := e.RebuildTop20Pool(effectMonth)
	if err != nil {
		return strategy.PoolVersion{}, err
	}
	pool, err = strategy.FreezePool(pool)
	if err != nil {
		return strategy.PoolVersion{}, err
	}
	if err := strategy.SavePool(e.DB, pool); err != nil {
		return strategy.PoolVersion{}, err
	}
	return pool, nil
}

// LoadPoolMonth 读取月份快照，无快照返回 ok=false。
func (e *Engine) LoadPoolMonth(effectMonth string) (strategy.PoolVersion, bool, error) {
	return strategy.LoadPool(e.DB, effectMonth)
}

// cachedPools 快照优先取池：有快照直接用，无则重建并落快照。
func (e *Engine) cachedPools(effectMonths []string) (map[string]strategy.PoolVersion, error) {
	pools := make(map[string]strategy.PoolVersion, len(effectMonths))
	for _, m := range effectMonths {
		if p, ok, err := e.LoadPoolMonth(m); err != nil {
			return nil, err
		} else if ok && len(p.Entries) > 0 {
			pools[m] = p
			continue
		}
		p, err := e.SnapshotTop20Pool(m)
		if err != nil {
			return nil, err
		}
		pools[m] = p
	}
	return pools, nil
}

// ReplayS12Cached 快照优先的 S1/S2/F1 重演：回放入口，供 HTTP 与后续任务复用。
func (e *Engine) ReplayS12Cached(
	ver strategy.S12Version, effectMonths []string, from, to string, market map[string]strategy.MarketInput,
	account strategy.AccountConfig,
) (strategy.S12Result, error) {
	pools, err := e.cachedPools(effectMonths)
	if err != nil {
		return strategy.S12Result{}, err
	}
	return e.replayS12Pools(ver, pools, from, to, market, account)
}

// PrepareS0 准备含正式日历的S0输入，计算前可冻结保存。
func (e *Engine) PrepareS0(
	effectMonths []string, from, to string, account strategy.AccountConfig, policies ...string,
) (strategy.S0Input, error) {
	if len(from) != 10 {
		return strategy.S0Input{}, fmt.Errorf("开始日期非法")
	}
	policy := replayPolicy(policies)
	p := strategy.DefaultParams()
	n := max(p.MaSlow, p.MaFast+p.SlopeN, p.MomentumN+1, p.AtrN+1, p.BreakoutN+1, p.AmountN+1)
	dates, _, decisionMonths, err := strategy.LoadReplaySchedule(e.DB, from, to, n)
	if err != nil {
		monthStart := from[:7] + "-01"
		var err2 error
		dates, _, err2 = strategy.LoadReplayCalendar(e.DB, monthStart, to)
		if err2 != nil {
			return strategy.S0Input{}, err
		}
	}
	pools, err := e.cachedPools(effectMonths)
	if err != nil {
		return strategy.S0Input{}, err
	}
	if decisionMonths != nil {
		for _, date := range dates {
			if date < from {
				continue
			}
			month := decisionMonths[date]
			if _, ok := pools[month]; !ok {
				extra, err := e.cachedPools([]string{month})
				if err != nil {
					return strategy.S0Input{}, err
				}
				pools[month] = extra[month]
			}
		}
	}
	warmFrom := dates[0]
	codes := strategy.PoolCodes(pools)
	bars, err := strategy.LoadStockBars(e.DB, codes,
		warmFrom, to, 800, policy == "available_candidates")
	if err != nil {
		return strategy.S0Input{}, err
	}
	quality, err := strategy.PrepareReplayBars(bars, dates, policy)
	if err != nil {
		return strategy.S0Input{}, err
	}
	acts, err := strategy.LoadCorporateActions(e.DB, codes, warmFrom, to)
	if err != nil {
		return strategy.S0Input{}, err
	}
	prices := make(map[string]map[string]strategy.S0Quote, len(bars))
	for code, pm := range bars {
		qm := make(map[string]strategy.S0Quote, len(pm))
		for d, b := range pm {
			qm[d] = strategy.S0Quote{Open: b.Open, Close: b.Close, AdjFactor: b.AdjFactor}
		}
		prices[code] = qm
	}
	in := strategy.S0Input{DataQuality: quality,
		Account: account, Params: p,
		Threshold:        strategy.DefaultThresholds(),
		Pools:            pools,
		Prices:           prices,
		Bars:             bars,
		CorporateActions: acts,
		Dates:            dates,
		StartDate:        from,
	}
	return in, nil
}

// prevMonthOf 取上月 YYYY-MM，非法返回空串。
func prevMonthOf(yearMonth string) string {
	if len(yearMonth) != 7 || yearMonth[4] != '-' {
		return ""
	}
	// 借用策略包的日期换算：月初首日的前一月即排名月。
	return strategy.PrevMonth(yearMonth + "-01")
}

// datasetVersion 数据版本标识：暂用常量，落快照后替换为快照版本。
func datasetVersion() string { return "daily_price@latest" }

// ReplayS12 历史回放：重建月份池→加载并校验原始Bar→指数→跑S1/S2/F1。
//
// Market 缺省不可用：未传市场状态时 S2/F1 全程禁新增（只跑明确标记的基础语义），
// S1 不受影响；ST/权限/停牌过滤后置，被困逻辑由 runner 内缺 Bar 语义承担。
func (e *Engine) ReplayS12(
	ver strategy.S12Version, effectMonths []string, from, to string, market map[string]strategy.MarketInput,
) (strategy.S12Result, error) {
	if len(effectMonths) == 0 || from == "" || to == "" || from > to {
		return strategy.S12Result{}, fmt.Errorf("回放参数非法")
	}
	pools := make(map[string]strategy.PoolVersion, len(effectMonths))
	for _, m := range effectMonths {
		p, err := e.RebuildTop20Pool(m)
		if err != nil {
			return strategy.S12Result{}, err
		}
		pools[m] = p
	}
	return e.replayS12Pools(ver, pools, from, to, market, strategy.DefaultAccount())
}

// prepareS12Pools 共用预热与回放装配，输入历史不改变账户开始日期。
func (e *Engine) prepareS12Pools(
	ver strategy.S12Version, pools map[string]strategy.PoolVersion, from, to string,
	market map[string]strategy.MarketInput, account strategy.AccountConfig, policies ...string,
) (strategy.S12Input, error) {
	policy := replayPolicy(policies)
	p := strategy.DefaultParams()
	n := max(p.MaSlow, p.MaFast+p.SlopeN, p.MomentumN+1, p.AtrN+1, p.BreakoutN+1, p.AmountN+1)
	dates, entryDays, decisionMonths, err := strategy.LoadReplaySchedule(e.DB, from, to, n)
	if err != nil {
		return strategy.S12Input{}, err
	}
	for _, date := range dates {
		if date < from {
			continue
		}
		month := decisionMonths[date]
		if _, ok := pools[month]; !ok {
			extra, err := e.cachedPools([]string{month})
			if err != nil {
				return strategy.S12Input{}, err
			}
			pools[month] = extra[month]
		}
	}
	warmFrom := dates[0]
	codes := strategy.PoolCodes(pools)
	bars, err := strategy.LoadStockBars(e.DB, codes,
		warmFrom, to, 800, policy == "available_candidates")
	if err != nil {
		return strategy.S12Input{}, err
	}
	quality, err := strategy.PrepareReplayBars(bars, dates, policy)
	if err != nil {
		return strategy.S12Input{}, err
	}
	acts, err := strategy.LoadCorporateActions(e.DB, codes, warmFrom, to)
	if err != nil {
		return strategy.S12Input{}, err
	}
	index, err := strategy.LoadIndexCloses(e.DB, "000300.SH", warmFrom, to, 800)
	if err != nil {
		return strategy.S12Input{}, err
	}
	if market == nil && (ver == strategy.VerS2 || ver == strategy.VerF1) {
		if mkt, err := strategy.LoadMarketInputs(e.DB, warmFrom, to); err == nil {
			market = mkt
		}
	}
	in := strategy.S12Input{DataQuality: quality,
		Account: account, Params: p,
		Threshold: strategy.DefaultThresholds(), Version: ver,
		Pools: pools, Bars: bars, Index: index, Market: market, Dates: dates, EntryDays: entryDays,
		StartDate: from, DecisionMonths: decisionMonths, CorporateActions: acts,
	}
	return in, nil
}

// PrepareS12 准备完整S12输入，计算前可冻结保存。
func (e *Engine) PrepareS12(ver strategy.S12Version, months []string, from, to string,
	market map[string]strategy.MarketInput, account strategy.AccountConfig, policies ...string,
) (strategy.S12Input, error) {
	pools, err := e.cachedPools(months)
	if err != nil {
		return strategy.S12Input{}, err
	}
	return e.prepareS12Pools(ver, pools, from, to, market, account, policies...)
}

// ReplayS0Cached 准备并运行默认模型的S0回放。
func (e *Engine) ReplayS0Cached(months []string, from, to string, account strategy.AccountConfig) (strategy.S0Result, error) {
	in, err := e.PrepareS0(months, from, to, account)
	if err != nil {
		return strategy.S0Result{}, err
	}
	res := strategy.RunS0Backtest(in)
	return res, res.Err
}

// replayS12Pools 准备并运行已选月池的S12回放。
func (e *Engine) replayS12Pools(ver strategy.S12Version, pools map[string]strategy.PoolVersion, from, to string,
	market map[string]strategy.MarketInput, account strategy.AccountConfig,
) (strategy.S12Result, error) {
	in, err := e.prepareS12Pools(ver, pools, from, to, market, account)
	if err != nil {
		return strategy.S12Result{}, err
	}
	res := strategy.RunS12Backtest(in)
	return res, res.Err
}

// replayPolicy 旧内部调用保持严格校验。
func replayPolicy(policies []string) string {
	if len(policies) > 0 && policies[0] != "" {
		return policies[0]
	}
	return "strict"
}
