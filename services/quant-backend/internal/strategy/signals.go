// 信号：市场状态与个股入场退出，对齐 spec/strategy/signals.md。
package strategy

// MarketState 市场状态枚举：按顺序首个命中生效。
type MarketState string

// 市场状态常量。
const (
	StateUnavailable MarketState = "unavailable" // 数据不可用：禁止新增。
	StateRetreat     MarketState = "retreat"     // 退潮：上限 0%。
	StateHot         MarketState = "hot"         // 过热分歧：上限 40% 且禁新增。
	StateTrend       MarketState = "trend"       // 趋势扩散：上限 80%。
	StateRepair      MarketState = "repair"      // 修复观察：上限 30%。
	StateOther       MarketState = "other"       // 其他：上限 20%。
)

// MarketInput 市场状态输入：B 宽度、V 成交、D 跌停压力、T 趋势是否成立。
type MarketInput struct {
	B         FactorResult // 当日宽度。
	BPrev     FactorResult // 前一日宽度（过热需 B 下降）。
	B5Ago     FactorResult // 5 日前宽度（趋势/修复比较）。
	V         FactorResult // 市场成交比。
	D         FactorResult // 跌停压力（S2 无 D 时视为缺失走基础分支）。
	Trend     bool         // 指数趋势 T 是否成立。
	DataOK    bool         // 必需行情与宽度是否通过质量门槛。
	HasD      bool         // 是否具备 D 数据（S3 完整表才为 true）。
	DownFromY bool         // B 较昨日是否下降。
}

// ClassifyState 按顺序匹配市场状态，首个命中生效。
func ClassifyState(in MarketInput, th MarketThresholds) MarketState {
	if !in.DataOK || !in.B.Available() {
		return StateUnavailable
	}
	if in.HasD {
		if in.B.Value < th.RetreatB {
			return StateRetreat
		}
		if in.D.Available() && in.D.Value >= 0.02 {
			return StateRetreat
		}
		if in.B.Value >= th.HotB && in.V.Available() && in.V.Value >= th.HotV && in.DownFromY {
			return StateHot
		}
	} else if in.B.Value < th.RetreatB {
		return StateRetreat
	}
	if in.Trend && in.B.Value >= th.TrendB && in.B5Ago.Available() && in.B.Value >= in.B5Ago.Value {
		return StateTrend
	}
	if in.B5Ago.Available() && in.B.Value >= th.RepairB && in.B.Value-in.B5Ago.Value >= th.RepairDelta5 {
		return StateRepair
	}
	return StateOther
}

// StateCap 状态对应总仓位上限。
func StateCap(s MarketState, th MarketThresholds) float64 {
	switch s {
	case StateRetreat, StateUnavailable:
		return th.RetreatCap
	case StateHot:
		return th.HotCap
	case StateTrend:
		return th.TrendCap
	case StateRepair:
		return th.RepairCap
	default:
		return th.OtherCap
	}
}

// EntryInput 个股入场输入：趋势、强度、突破、量能、追高过滤均已算好。
type EntryInput struct {
	CloseGTMa20  bool         // 收盘>MA20。
	Ma20GTMa60   bool         // MA20>MA60。
	SlopeUp      bool         // MA20 高于 5 日前。
	RS           FactorResult // RS20>0。
	Breakout     bool         // 收盘突破前 20 日最高收盘。
	AmountRatio  FactorResult // 量能比 1.2~2.5 边界包含。
	NotExtended  bool         // 未超过 MA20+3ATR。
	HistoryOK    bool         // 连续窗口完整（停牌缺失不得凑数）。
	Tradable     bool         // 权限/停牌/规则通过。
	CooldownOver bool         // 已过 5 日冷却。
}

// EntrySignal S1/S2 共用入场：全部成立才触发，不做线性加权。
func EntrySignal(e EntryInput, p StrategyParams) bool {
	if !e.HistoryOK || !e.Tradable || !e.CooldownOver {
		return false
	}
	if !(e.CloseGTMa20 && e.Ma20GTMa60 && e.SlopeUp) {
		return false
	}
	if !e.RS.Available() || e.RS.Value <= 0 {
		return false
	}
	if !e.Breakout {
		return false
	}
	if !e.AmountRatio.Available() || e.AmountRatio.Value < p.AmountLow || e.AmountRatio.Value > p.AmountHigh {
		return false
	}
	return e.NotExtended
}

// ExitInput 个股退出输入：离池、跌破退出线、连续收于 MA20 下。
type ExitInput struct {
	OffPool     bool // 离开新月原始 Top20。
	BelowStop   bool // 收盘跌破有效退出线（次日执行，不回填当日）。
	BelowMaDays int  // 连续收于 MA20 下天数。
	MaExitDays  int  // 阈值，默认 2。
	DataRisk    bool // 存量指标算不出：数据风险退出。
	AccountHalt bool // 账户 20% 停用：全平。
	MarketCut   bool // 市场降仓：按比例减仓（非全平由组合层处理）。
}

// ExitReason 退出原因：并存时按 execution.md 优先级取最高者。
type ExitReason string

// 退出原因常量。
const (
	ExitNone    ExitReason = ""        // 不退出。
	ExitHalt    ExitReason = "halt"    // 账户停用/数据风险。
	ExitOffPool ExitReason = "offpool" // 离池/资格失效。
	ExitStop    ExitReason = "stop"    // 跌破退出线。
	ExitMa      ExitReason = "ma"      // 连续收于 MA20 下。
	ExitMarket  ExitReason = "market"  // 市场降仓。
	ExitRisk    ExitReason = "risk"    // 账户计划风险减仓。
)

// ExitSignal 退出优先级：停用/数据风险 > 离池 > 市场降仓 > 跌破 > 均线。
func ExitSignal(e ExitInput) ExitReason {
	if e.AccountHalt || e.DataRisk {
		return ExitHalt
	}
	if e.OffPool {
		return ExitOffPool
	}
	if e.MarketCut {
		return ExitMarket
	}
	if e.BelowStop {
		return ExitStop
	}
	if e.MaExitDays > 0 && e.BelowMaDays >= e.MaExitDays {
		return ExitMa
	}
	return ExitNone
}

// exitPriority 退出理由优先级数值：halt > offpool > market > stop > ma > risk > none。
func exitPriority(r ExitReason) int {
	switch r {
	case ExitHalt:
		return 5
	case ExitOffPool:
		return 4
	case ExitMarket:
		return 3
	case ExitStop:
		return 2
	case ExitMa:
		return 1
	case ExitRisk:
		return 0
	default:
		return -1
	}
}
