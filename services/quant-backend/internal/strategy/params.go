// Package strategy 参数登记：对齐 spec/strategy/parameters.md 与 account.md v0.4。
package strategy

import "math"

// AccountConfig 账户情景：本金、额度、风险、回撤、目标，金额以元为单位。
type AccountConfig struct {
	InitialCapital float64 `json:"initial_capital"`  // C0 期初现金，默认 500000。
	GrossCapMode   string  `json:"gross_cap_mode"`   // nav_fraction / fixed_amount / both。
	GrossCapRatio  float64 `json:"gross_cap_ratio"`  // 股票总市值占净资产上限，默认 0.80。
	GrossCapAmount float64 `json:"gross_cap_amount"` // 绝对金额上限，未设置填 0。
	CashReserve    float64 `json:"cash_reserve"`     // 必须保留现金，默认 0。
	SingleCapRatio float64 `json:"single_cap_ratio"` // 单股占净资产上限，默认 0.40。
	SingleRisk     float64 `json:"single_risk"`      // 单股计划损失占净资产，默认 0.01。
	TotalRisk      float64 `json:"total_risk"`       // 组合计划损失占净资产，默认 0.02。
	MaxPositions   int     `json:"max_positions"`    // 最多持仓数 1 或 2，默认 2。
	DDReduce       float64 `json:"dd_reduce"`        // 回撤减半阈值，默认 0.10。
	DDFreeze       float64 `json:"dd_freeze"`        // 回撤禁新增阈值，默认 0.15。
	DDHalt         float64 `json:"dd_halt"`          // 回撤停用阈值，默认 0.20。
	TargetMultiple float64 `json:"target_multiple"`  // 翻倍目标倍数，默认 2。
}

// StrategyParams 策略参数：窗口、触发、退出、费用与执行，均为研究初值。
type StrategyParams struct {
	MaFast        int     `json:"ma_fast"`         // 短均线，默认 20。
	MaSlow        int     `json:"ma_slow"`         // 长均线，默认 60。
	AtrN          int     `json:"atr_n"`           // ATR 窗口，默认 20。
	MomentumN     int     `json:"momentum_n"`      // 动量窗口，默认 20。
	SlopeN        int     `json:"slope_n"`         // MA20 斜率比较天数，默认 5。
	BreakoutN     int     `json:"breakout_n"`      // 突破前高收盘窗口，默认 20。
	AmountN       int     `json:"amount_n"`        // 量能均值窗口，默认 20。
	AmountLow     float64 `json:"amount_low"`      // 量能比下限，默认 1.2。
	AmountHigh    float64 `json:"amount_high"`     // 量能比上限，默认 2.5。
	MaxExtension  float64 `json:"max_extension"`   // 追高上限 MA20+3ATR，默认 3。
	InitStopAtr   float64 `json:"init_stop_atr"`   // 初始退出倍数，默认 2.5。
	TrailAtr      float64 `json:"trail_atr"`       // 跟踪退出倍数，默认 2.5。
	MaExitDays    int     `json:"ma_exit_days"`    // 连续收于 MA20 下退出天数，默认 2。
	CooldownDays  int     `json:"cooldown_days"`   // 完全退出后冷却交易日，默认 5。
	GapUpLimit    float64 `json:"gap_up_limit"`    // 开盘跳空取消上限，默认 0.03。
	Commission    float64 `json:"commission"`      // 佣金率，默认 0.0003。
	MinCommission float64 `json:"min_commission"`  // 最低佣金，默认 5。
	SlippageBps   float64 `json:"slippage_bps"`    // 单边滑点基点，默认 10。
	StampRate     float64 `json:"stamp_rate"`      // 印花税率卖出单边，研究情景默认 0.0005。
}

// MarketThresholds 市场状态阈值：对齐 signals.md S3 完整表，小数比例。
type MarketThresholds struct {
	RetreatB     float64 // 退潮 B<0.35。
	HotB         float64 // 过热 B>=0.75。
	HotV         float64 // 过热 V>=1.8。
	TrendB       float64 // 趋势 B>=0.55。
	RepairB      float64 // 修复 B>=0.40。
	RepairDelta5 float64 // 修复 ΔB5>=0.05。
	RetreatCap   float64 // 退潮上限 0。
	HotCap       float64 // 过热上限 0.40。
	TrendCap     float64 // 趋势上限 0.80。
	RepairCap    float64 // 修复上限 0.30。
	OtherCap     float64 // 其他上限 0.20。
}

// DefaultAccount 默认 50 万元、最多 2 只账户情景。
func DefaultAccount() AccountConfig {
	return AccountConfig{InitialCapital: 500000, GrossCapMode: "nav_fraction", GrossCapRatio: 0.80,
		GrossCapAmount: 0, CashReserve: 0, SingleCapRatio: 0.40, SingleRisk: 0.01,
		TotalRisk: 0.02, MaxPositions: 2, DDReduce: 0.10, DDFreeze: 0.15, DDHalt: 0.20, TargetMultiple: 2}
}

// DefaultParams 默认策略参数。
func DefaultParams() StrategyParams {
	return StrategyParams{MaFast: 20, MaSlow: 60, AtrN: 20, MomentumN: 20, SlopeN: 5, BreakoutN: 20,
		AmountN: 20, AmountLow: 1.2, AmountHigh: 2.5, MaxExtension: 3, InitStopAtr: 2.5, TrailAtr: 2.5,
		MaExitDays: 2, CooldownDays: 5, GapUpLimit: 0.03, Commission: 0.0003, MinCommission: 5, SlippageBps: 10,
		StampRate: 0.0005}
}

// DefaultThresholds 默认市场状态阈值。
func DefaultThresholds() MarketThresholds {
	return MarketThresholds{RetreatB: 0.35, HotB: 0.75, HotV: 1.8, TrendB: 0.55, RepairB: 0.40,
		RepairDelta5: 0.05, RetreatCap: 0, HotCap: 0.40, TrendCap: 0.80, RepairCap: 0.30, OtherCap: 0.20}
}

// ValidateAccount 校验账户输入：比例 0~1、C0>0、0<=R<C0、回撤严格递增。
func ValidateAccount(a AccountConfig) error {
	for _, v := range []float64{a.InitialCapital, a.GrossCapRatio, a.GrossCapAmount, a.CashReserve,
		a.SingleCapRatio, a.SingleRisk, a.TotalRisk, a.DDReduce, a.DDFreeze, a.DDHalt, a.TargetMultiple} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return errInvalid("账户数值须为有限数")
		}
	}
	if a.InitialCapital <= 0 || a.CashReserve < 0 || a.CashReserve >= a.InitialCapital {
		return errInvalid("本金须>0 且保留现金须满足 0<=R<C0")
	}
	for _, v := range []float64{a.GrossCapRatio, a.SingleCapRatio, a.SingleRisk, a.TotalRisk} {
		if v < 0 || v > 1 {
			return errInvalid("账户比例须为 0~1 有限数")
		}
	}
	if a.GrossCapAmount < 0 || a.TargetMultiple <= 0 {
		return errInvalid("持仓金额上限须非负，目标倍数须为正")
	}
	switch a.GrossCapMode {
	case "nav_fraction":
	case "fixed_amount", "both":
		if a.GrossCapAmount <= 0 {
			return errInvalid("固定金额或双重上限模式须设置正金额额度")
		}
	default:
		return errInvalid("持仓额度模式非法")
	}
	if !(0 <= a.DDReduce && a.DDReduce < a.DDFreeze && a.DDFreeze < a.DDHalt && a.DDHalt <= 1) {
		return errInvalid("回撤阈值须在0~1内且严格递增")
	}
	if a.MaxPositions != 1 && a.MaxPositions != 2 {
		return errInvalid("最多持仓只允许 1 或 2")
	}
	return nil
}

// ValidateParams 校验策略参数：周期为正、均线递增、量比合理、费用有限且非负。
func ValidateParams(p StrategyParams) error {
	for _, v := range []float64{p.AmountLow, p.AmountHigh, p.MaxExtension, p.InitStopAtr, p.TrailAtr,
		p.GapUpLimit, p.Commission, p.MinCommission, p.SlippageBps, p.StampRate} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return errInvalid("策略参数浮点数值须为有限数")
		}
	}
	for _, n := range []int{p.MaFast, p.MaSlow, p.AtrN, p.MomentumN, p.SlopeN, p.BreakoutN, p.AmountN, p.MaExitDays} {
		if n <= 0 {
			return errInvalid("策略窗口与周期须为正整数")
		}
	}
	if p.MaSlow <= p.MaFast {
		return errInvalid("长均线周期须大于短均线周期")
	}
	if p.CooldownDays < 0 {
		return errInvalid("冷却交易日须非负")
	}
	if p.AmountLow <= 0 || p.AmountHigh <= p.AmountLow {
		return errInvalid("量能倍数须为正且上限大于下限")
	}
	if p.InitStopAtr <= 0 || p.TrailAtr <= 0 || p.MaxExtension <= 0 || p.GapUpLimit <= 0 {
		return errInvalid("止损ATR倍数、追高上限及跳空限制须为正数")
	}
	if p.Commission < 0 || p.MinCommission < 0 || p.SlippageBps < 0 || p.StampRate < 0 {
		return errInvalid("费用参数须非负")
	}
	return nil
}

