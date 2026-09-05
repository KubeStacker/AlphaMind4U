// 存量风险减仓：按信号日计划损失缩减，受阻卖单不从实际账本消失。
package strategy

import (
	"fmt"
	"math"
)

// riskKeptShares 只缩减已有数量，最低佣金导致最小持仓也超限时返回全平。
func riskKeptShares(shares, lot int, price, stop, budget float64, p StrategyParams) int {
	if positionRisk(shares, price, stop, p) <= budget {
		return shares
	}
	lo, hi := 0, shares/lot
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if positionRisk(mid*lot, price, stop, p) <= budget {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo * lot
}

// queueRiskReductions 先限制单股风险，再按比例缩减组合风险，不释放未成交库存。
func queueRiskReductions(led *Ledger, closes map[string]float64, a AccountConfig, p StrategyParams,
	date string, lot func(string) int, sells map[string]pendingSell,
) error {
	nav := led.NAV(closes)
	if math.IsNaN(nav) || math.IsInf(nav, 0) {
		return fmt.Errorf("%s: 风险减仓净资产非法，回放阻断", date)
	}
	codes := sortedHoldings(led)
	kept := make(map[string]int, len(codes))
	risks := make(map[string]float64, len(codes))
	sum := 0.0
	for _, code := range codes {
		h := led.Holdings[code]
		remaining := h.Shares - min(h.Shares, sells[code].Shares)
		if remaining <= 0 {
			continue
		}
		price, stop, unit := closes[code], h.Stop, lot(code)
		if price <= 0 || stop <= 0 || math.IsNaN(price) || math.IsNaN(stop) ||
			math.IsInf(price, 0) || math.IsInf(stop, 0) || unit <= 0 {
			return fmt.Errorf("%s %s: 风险减仓价格/退出线/单位非法，回放阻断", code, date)
		}
		if price <= stop {
			sells[code] = mergePendingSell(sells[code], pendingSell{TsCode: code, Shares: h.Shares,
				Reason: ExitStop, SignalDate: date})
			continue
		}
		if risk := positionRisk(remaining, price, stop, p); math.IsInf(risk, 0) || math.IsNaN(risk) {
			return fmt.Errorf("%s %s: 存量计划风险非法，回放阻断", code, date)
		}
		kept[code] = riskKeptShares(remaining, unit, price, stop, math.Max(0, nav*a.SingleRisk), p)
		risks[code] = positionRisk(kept[code], price, stop, p)
		sum += risks[code]
	}
	scale := 1.0
	if total := math.Max(0, nav*a.TotalRisk); sum > total {
		scale = total / sum
	}
	for _, code := range codes {
		keep, ok := kept[code]
		if !ok {
			continue
		}
		h := led.Holdings[code]
		keep = riskKeptShares(keep, lot(code), closes[code], h.Stop, risks[code]*scale, p)
		if shares := h.Shares - keep; shares > sells[code].Shares {
			sells[code] = mergePendingSell(sells[code], pendingSell{TsCode: code, Shares: shares,
				Reason: ExitRisk, SignalDate: date})
		}
	}
	return nil
}
