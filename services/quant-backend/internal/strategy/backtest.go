// 回测账本：日频事件顺序、次日开盘近似成交、费用与漏斗，对齐 execution.md。
package strategy

import (
	"math"
)

// Intent 买卖意图：信号日冻结，次日开盘参考窗口执行，只收紧不放宽。
type Intent struct {
	SignalDate string  // 信号日。
	Side       string  // buy / sell。
	TsCode     string  // 证券代码。
	Shares     int     // 冻结股数（买）或目标卖量（卖）。
	RefPrice   float64 // 信号参考价（买为信号收盘）。
	StopPrice  float64 // 初始退出线（买单携带）。
	MarketCap  float64 // 买单信号日冻结的市场/回撤比例上限，卖单不适用。
	Reason     ExitReason
}

// Fill 成交结果：参考开盘价加减滑点，不利方向取整由调用方按最小价位处理。
type Fill struct {
	TsCode string  // 证券代码。
	Shares int     // 实际成交股数。
	Price  float64 // 实际成交价（含滑点）。
	Fee    float64 // 佣金+印花税+过户费合计（调用方按费率表计）。
}

// CorporateActionFill 记录公司行动导致的现金到账与股份变化。
type CorporateActionFill struct {
	Date        string  `json:"date"`         // 除权/派息日。
	TsCode      string  `json:"ts_code"`      // 证券代码。
	CashDiv     float64 `json:"cash_div"`     // 现金分红到账总额。
	BonusShares int     `json:"bonus_shares"` // 送转股增加股数。
	CashAfter   float64 `json:"cash_after"`   // 分红后账户现金。
}

// Ledger 账户账本：现金、持仓、费用与净值高水位。
type Ledger struct {
	Fills             []TradeFill           // 按实际执行顺序保存的成交。
	CorporateActions  []CorporateActionFill // 公司行动（分红派息、送转）记录。
	Cash              float64               // 可用现金。
	Holdings          map[string]*Holding   // 代码→持仓。
	FeesPaid          float64               // 累计费用。
	DividendsReceived float64               // 累计现金分红。
	PeakNAV           float64               // 历史最高净资产。
}

// recordCorporateAction 记录除权除息导致的现金分红与送转股增加。
func (l *Ledger) recordCorporateAction(date, code string, cashDiv float64, bonusShares int) {
	l.CorporateActions = append(l.CorporateActions, CorporateActionFill{
		Date:        date,
		TsCode:      code,
		CashDiv:     cashDiv,
		BonusShares: bonusShares,
		CashAfter:   l.Cash,
	})
}

// Holding 单只持仓：股数、成本与退出线（复权尺度换算后）。
type Holding struct {
	TsCode  string  // 证券代码。
	Shares  int     // 持股数。
	AvgCost float64 // 均价（原价尺度）。
	Stop    float64 // 有效退出线。
	High    float64 // 入场以来最高复权收盘。
}

// Funnel 机会与资金漏斗：每层独立拒绝数与顺序剩余，不可相加当总数。
type Funnel struct {
	Raw20      int // 原始 Top20。
	Tradable   int // 权限/历史后可交易。
	MarketPass int // 市场上限后仍有预算。
	SignalHit  int // 触发个股信号。
	Filled     int // 实际成交。
	NoCash     int // 现金/名额不足拒绝。
	Rejected   int // 追价/涨跌停/跳空取消。
}

// NewLedger 新账本：现金 C0，零持仓，不继承他户高水位。
func NewLedger(c0 float64) *Ledger {
	return &Ledger{Cash: c0, Holdings: map[string]*Holding{}, PeakNAV: c0}
}

// NAV 净值：现金+持股按给定收盘估值，停牌用最后可用价并由调用方标陈旧。
func (l *Ledger) NAV(closes map[string]float64) float64 {
	nav := l.Cash
	for code, h := range l.Holdings {
		if px, ok := closes[code]; ok {
			nav += float64(h.Shares) * px
		}
	}
	return nav
}

// MarkPeak 更新历史最高净值。
func (l *Ledger) MarkPeak(nav float64) {
	if nav > l.PeakNAV {
		l.PeakNAV = nav
	}
}

// BuyFillPrice 买入预计成交价=开盘参考*(1+滑点)，卖出反向。
func BuyFillPrice(openRef float64, p StrategyParams) float64 {
	return openRef * (1 + p.SlippageBps/10000)
}

// SellFillPrice 卖出预计成交价=开盘参考*(1-滑点)。
func SellFillPrice(openRef float64, p StrategyParams) float64 {
	return openRef * (1 - p.SlippageBps/10000)
}

// CheckBuyGate 执行前核验：涨跌停端点、3%跳空、偏离上限、退出线有效性。
func CheckBuyGate(signalClose, openRef, fillPrice, stopPrice, ma20, atr float64, atLimit bool, p StrategyParams) string {
	if atLimit {
		return "开盘涨跌停不新买"
	}
	if signalClose > 0 && openRef/signalClose-1 > p.GapUpLimit {
		return "向上跳空超3%取消"
	}
	if !(fillPrice > stopPrice) {
		return "预计成交价须高于初始退出线"
	}
	if atr > 0 && fillPrice > ma20+p.MaxExtension*atr {
		return "超过均线偏离上限"
	}
	if math.IsNaN(fillPrice) || fillPrice <= 0 {
		return "价格非法"
	}
	return ""
}

// ApplyBuy 现金扣减与持仓建仓：T+1 可卖量由调用方按日期管理。
func (l *Ledger) ApplyBuy(code string, shares int, price, fee float64, stop float64) {
	l.Cash -= float64(shares)*price + fee
	l.FeesPaid += fee
	if h, ok := l.Holdings[code]; ok {
		total := float64(h.Shares)*h.AvgCost + float64(shares)*price
		h.Shares += shares
		h.AvgCost = total / float64(h.Shares)
		if stop > h.Stop {
			h.Stop = stop
		}
		if price > h.High {
			h.High = price
		}
		return
	}
	l.Holdings[code] = &Holding{TsCode: code, Shares: shares, AvgCost: price, Stop: stop, High: price}
}

// ApplySell 持仓扣减与现金到账：未成交部分保留原退出原因由调用方重试。
func (l *Ledger) ApplySell(code string, shares int, price, fee float64) {
	h, ok := l.Holdings[code]
	if !ok {
		return
	}
	if shares > h.Shares {
		shares = h.Shares
	}
	h.Shares -= shares
	l.Cash += float64(shares)*price - fee
	l.FeesPaid += fee
	if h.Shares <= 0 {
		delete(l.Holdings, code)
	}
}
