// 回撤状态：run内锁定最高档位，满足连续恢复条件后才解除非停用状态。
package strategy

// drawdownTracker 保存已触发档位与连续恢复天数，不能在换月或平仓时重置。
type drawdownTracker struct {
	level        int
	recoveryDays int
}

// update 每个回放交易日收盘调用一次，市场未知不能作为恢复依据。
func (d *drawdownTracker) update(nav, peak float64, account AccountConfig, marketOK bool) DrawdownState {
	current := account.DrawdownCover(nav, peak)
	level := 0
	switch {
	case current.Halt:
		level = 3
	case current.FreezeNew:
		level = 2
	case current.CapFactor < 1:
		level = 1
	}
	if level > d.level {
		d.level = level
	}
	if d.level == 3 {
		return DrawdownState{Halt: true}
	}
	if d.level > 0 && level == 0 && peak > 0 && 1-nav/peak < 0.08 && marketOK {
		d.recoveryDays++
		if d.recoveryDays >= 5 {
			d.level, d.recoveryDays = 0, 0
		}
	} else {
		d.recoveryDays = 0
	}
	switch d.level {
	case 2:
		return DrawdownState{CapFactor: 1, FreezeNew: true}
	case 1:
		return DrawdownState{CapFactor: 0.5}
	default:
		return DrawdownState{CapFactor: 1}
	}
}

// recoveryMarketOK 只接受有质量标记、可分类且非退潮的市场输入。
func recoveryMarketOK(markets map[string]MarketInput, date string, th MarketThresholds) bool {
	mi, ok := markets[date]
	if !ok {
		return false
	}
	state := ClassifyState(mi, th)
	return state != StateUnavailable && state != StateRetreat
}
