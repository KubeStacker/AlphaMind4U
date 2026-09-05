// 趋势装配：由预计算因子组装 S1/S2 入场输入，对齐 signals.md。
package strategy

// TrendSnapshot 单股单日趋势快照：调用方保证窗口连续，缺失用 FactorResult 标记。
type TrendSnapshot struct {
	Close    float64      // 信号日收盘。
	MA20     FactorResult // 20 日均线。
	MA60     FactorResult // 60 日均线。
	MA20Prev FactorResult // 5 日前 MA20（斜率比较）。
	RS       FactorResult // RS20。
	HC20     FactorResult // 前 20 日最高收盘。
	AmountR  FactorResult // 量能比。
	ATR      FactorResult // ATR20。
}

// BuildEntry 由快照组装入场输入：突破=收盘>HC20，追高=收盘<=MA20+3ATR。
func BuildEntry(s TrendSnapshot, historyOK, tradable, cooldownOver bool, p StrategyParams) EntryInput {
	e := EntryInput{HistoryOK: historyOK, Tradable: tradable, CooldownOver: cooldownOver}
	if s.MA20.Available() && s.MA60.Available() {
		e.CloseGTMa20 = s.Close > s.MA20.Value
		e.Ma20GTMa60 = s.MA20.Value > s.MA60.Value
	}
	if s.MA20.Available() && s.MA20Prev.Available() {
		e.SlopeUp = s.MA20.Value > s.MA20Prev.Value
	}
	e.RS = s.RS
	if s.HC20.Available() {
		e.Breakout = s.Close > s.HC20.Value
	}
	e.AmountRatio = s.AmountR
	e.NotExtended = true
	if s.MA20.Available() && s.ATR.Available() {
		e.NotExtended = s.Close <= s.MA20.Value+p.MaxExtension*s.ATR.Value
	}
	return e
}

// MarketBudget 市场状态换算可用比例：状态上限×回撤乘数，禁新增直接归零可用。
func MarketBudget(s MarketState, th MarketThresholds, dd DrawdownState) float64 {
	if s == StateHot || s == StateUnavailable || s == StateRetreat || dd.FreezeNew || dd.Halt {
		// 过热禁新增、数据不可用、退潮、回撤冻结/停用：禁止新增，存量按风险规则处理。
		return 0
	}
	return StateCap(s, th) * dd.CapFactor
}
