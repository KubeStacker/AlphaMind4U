// 组合：预算公式、风险定仓、回撤覆盖，对齐 spec/strategy/portfolio.md 与 account.md。
package strategy

import (
	"math"
)

// Position 现有持仓：市值、退出线与计划风险估算输入。
type Position struct {
	TsCode   string  // 证券代码。
	Shares   int     // 持股数（含零股与被困仓，同样占名额）。
	MktValue float64 // 最新市值。
	StopDist float64 // 至有效退出线正距离比例，如 0.08。
}

// BudgetInput 预算输入：净资产、现金、持仓市值与各类上限。
type BudgetInput struct {
	NAV       float64 // 完整账户净资产 E。
	Cash      float64 // 已到账可用现金。
	HeldValue float64 // 当前股票市值 X。
	MarketCap float64 // 市场/回撤允许比例 p。
	Account   AccountConfig
}

// TargetBudget 股票目标总预算 B=min(B_user,p*E,max(0,E-R))，再扣已持仓。
func TargetBudget(in BudgetInput) (total, avail float64) {
	a := in.Account
	var bUser float64
	switch a.GrossCapMode {
	case "fixed_amount":
		bUser = a.GrossCapAmount
	case "both":
		bUser = math.Min(a.GrossCapRatio*in.NAV, a.GrossCapAmount)
	default:
		bUser = a.GrossCapRatio * in.NAV
	}
	total = math.Min(bUser, in.MarketCap*in.NAV)
	if cap := in.NAV - a.CashReserve; cap < total {
		total = cap
	}
	if total < 0 {
		total = 0
	}
	avail = total - in.HeldValue
	if avail < 0 {
		avail = 0
	}
	cashAvail := in.Cash - a.CashReserve
	if cashAvail < avail {
		avail = math.Max(0, cashAvail)
	}
	return total, avail
}

// SizeShares 风险定仓：取等份预算、40%上限、1%风险/退出距离三者最小，再向下取整手。
func SizeShares(nav, budgetEach, stopDist float64, price float64, a AccountConfig, lot int) (shares int, reason string) {
	if price <= 0 || lot <= 0 {
		return 0, "价格或交易单位非法"
	}
	if stopDist <= 0 {
		return 0, "退出距离须为正"
	}
	byBudget := budgetEach
	byCap := nav * a.SingleCapRatio
	byRisk := nav * a.SingleRisk / stopDist
	target := math.Min(byBudget, math.Min(byCap, byRisk))
	lots := int(math.Floor(target / (price * float64(lot))))
	if lots < 1 {
		return 0, "最小申报金额超过预算"
	}
	return lots * lot, ""
}

// DrawdownState 回撤覆盖输出：上限乘数与是否禁新增/停用。
type DrawdownState struct {
	CapFactor float64 // 市场上限乘数：10% 回撤减半。
	FreezeNew bool    // 15% 回撤禁新增且上限最高 20%。
	Halt      bool    // 20% 回撤停用。
}

// TargetCap 回撤覆盖后的存量目标比例，与是否允许新买分开计算。
func (d DrawdownState) TargetCap(marketCap float64) float64 {
	if d.Halt {
		return 0
	}
	if d.FreezeNew {
		return math.Min(marketCap, 0.20)
	}
	return marketCap * d.CapFactor
}

// DrawdownCover 按默认账户计算回撤覆盖，锁定最高严重程度由调用方保持。
func DrawdownCover(nav, peak float64) DrawdownState {
	return DefaultAccount().DrawdownCover(nav, peak)
}

// DrawdownCover 按已校验账户的回撤阈值计算当前覆盖状态，不负责锁定与解除。
func (a AccountConfig) DrawdownCover(nav, peak float64) DrawdownState {
	if peak <= 0 || nav > peak {
		return DrawdownState{CapFactor: 1}
	}
	// 浮点容差：恰好达到配置阈值时应触发对应档位。
	const eps = 1e-9
	dd := 1 - nav/peak
	switch {
	case dd+eps >= a.DDHalt:
		return DrawdownState{Halt: true}
	case dd+eps >= a.DDFreeze:
		return DrawdownState{CapFactor: 1, FreezeNew: true}
	case dd+eps >= a.DDReduce:
		return DrawdownState{CapFactor: 0.5}
	default:
		return DrawdownState{CapFactor: 1}
	}
}

// CountPositions 持仓计数：停牌/跌停/零股同样占名额，超 2 即账本错误。
func CountPositions(pos []Position) int { return len(pos) }

// CommissionFee 佣金：按率双向计，不足 5 元按 5 元，接到真实费率前为研究情景。
func CommissionFee(amount float64, p StrategyParams) float64 {
	f := amount * p.Commission
	if f < p.MinCommission {
		return p.MinCommission
	}
	return f
}

// SlippageCost 滑点成本：单边 10bp，金额*0.001。
func SlippageCost(amount float64, p StrategyParams) float64 { return amount * p.SlippageBps / 10000 }
