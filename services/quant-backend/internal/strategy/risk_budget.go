// 新买计划风险：存量、同批待买与费用一起约束，实际亏损仍可能超过计划。
package strategy

import "math"

// positionRisk 估值至滑点后退出价的计划损失及卖出费用，非法或破线时返回不可用。
func positionRisk(shares int, price, stop float64, p StrategyParams) float64 {
	if shares <= 0 {
		return 0
	}
	for _, v := range []float64{price, stop, p.Commission, p.MinCommission, p.StampRate, p.SlippageBps} {
		if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return math.Inf(1)
		}
	}
	if stop <= 0 || price <= stop || p.SlippageBps >= 10000 {
		return math.Inf(1)
	}
	exit := SellFillPrice(stop, p)
	amount := float64(shares) * exit
	return float64(shares)*(price-exit) + CommissionFee(amount, p) + amount*p.StampRate
}

// remainingRisk 存量全数占用组合额度，未成交退出不提前释放风险。
func remainingRisk(led *Ledger, prices map[string]float64, nav float64, a AccountConfig, p StrategyParams) float64 {
	if nav <= 0 || math.IsNaN(nav) || math.IsInf(nav, 0) {
		return 0
	}
	room := nav * a.TotalRisk
	for _, code := range sortedHoldings(led) {
		h := led.Holdings[code]
		risk := positionRisk(h.Shares, prices[code], h.Stop, p)
		if risk > nav*a.SingleRisk {
			return 0
		}
		room -= risk
	}
	return math.Max(0, room)
}

// sizeRiskBuy 在已有数量上限内二分缩量，佣金最低额也纳入现金和风险约束。
func sizeRiskBuy(maxShares, lot int, price, stop, nav, budget, room float64,
	a AccountConfig, p StrategyParams,
) (shares int, risk, cost float64) {
	if maxShares <= 0 || lot <= 0 || price <= 0 || nav <= 0 || budget <= 0 || room <= 0 {
		return 0, 0, 0
	}
	cap := math.Min(nav*a.SingleRisk, room)
	lo, hi := 0, maxShares/lot
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		qty := mid * lot
		amount := float64(qty) * price
		fee := CommissionFee(amount, p)
		planned := positionRisk(qty, price, stop, p) + fee
		if planned <= cap && amount+fee <= budget && amount <= nav*a.SingleCapRatio {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	if lo == 0 {
		return 0, 0, 0
	}
	shares = lo * lot
	amount := float64(shares) * price
	fee := CommissionFee(amount, p)
	return shares, positionRisk(shares, price, stop, p) + fee, amount + fee
}

// buyBudgetAllowed 按开盘估值和冻结市场上限复核资金及风险，不临时扩额或改量。
func buyBudgetAllowed(led *Ledger, prices map[string]float64, code string, shares int,
	price, stop, marketCap float64, a AccountConfig, p StrategyParams,
) bool {
	if marketCap <= 0 || marketCap > 1 || math.IsNaN(marketCap) {
		return false
	}
	fee := CommissionFee(float64(shares)*price, p)
	markedNAV := led.NAV(prices)
	nav := markedNAV - fee - float64(shares)*math.Max(0, price-prices[code])
	_, available := TargetBudget(BudgetInput{NAV: nav, Cash: led.Cash, HeldValue: markedNAV - led.Cash,
		MarketCap: marketCap, Account: a})
	room := remainingRisk(led, prices, nav, a, p)
	risk := positionRisk(shares, price, stop, p) + fee
	return shares > 0 && nav > 0 && risk <= math.Min(room, nav*a.SingleRisk) &&
		float64(shares)*price <= nav*a.SingleCapRatio && float64(shares)*price+fee <= available
}
