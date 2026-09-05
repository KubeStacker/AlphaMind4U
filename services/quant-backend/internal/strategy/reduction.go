// 减仓装配：按收盘目标冻结次日卖量，不预支未成交卖单的现金。
package strategy

import (
	"fmt"
	"math"
)

// queueReductions 先考虑已冻结退出，再按单股和总市值预算收紧保留数量。
func queueReductions(led *Ledger, closes map[string]float64, account AccountConfig,
	marketCap float64, date string, lot func(string) int, sells map[string]pendingSell,
) error {
	codes := sortedHoldings(led)
	nav := led.NAV(closes)
	total, _ := TargetBudget(BudgetInput{NAV: nav, Cash: led.Cash, HeldValue: nav - led.Cash,
		MarketCap: marketCap, Account: account})
	desired := make(map[string]float64, len(codes))
	sum := 0.0
	for _, code := range codes {
		h := led.Holdings[code]
		remaining := h.Shares - min(h.Shares, sells[code].Shares)
		if remaining <= 0 {
			continue
		}
		px := closes[code]
		if px <= 0 || math.IsNaN(px) || math.IsInf(px, 0) {
			return fmt.Errorf("%s %s: 减仓估值缺失/非法，回放阻断", code, date)
		}
		desired[code] = math.Min(float64(remaining)*px, math.Max(0, nav*account.SingleCapRatio))
		sum += desired[code]
	}
	scale := 1.0
	if sum > total {
		scale = total / sum
	}
	for _, code := range codes {
		h := led.Holdings[code]
		remaining := h.Shares - min(h.Shares, sells[code].Shares)
		if remaining <= 0 {
			continue
		}
		px := closes[code]
		target := desired[code] * scale
		if target >= float64(remaining)*px {
			continue
		}
		unit := lot(code)
		if unit <= 0 {
			return fmt.Errorf("%s %s: 减仓交易单位非法，回放阻断", code, date)
		}
		keep := min(remaining, int(math.Floor(target/px/float64(unit)))*unit)
		sells[code] = mergePendingSell(sells[code], pendingSell{TsCode: code, Shares: h.Shares - keep,
			Reason: ExitMarket, SignalDate: date})
	}
	return nil
}
