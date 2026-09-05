// Strategy 同步回测与股票池快照只读接口：参数强校验，结果直接返回。
package api

import (
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"quantbackend/internal/strategy"
)

// poolsOf 输出本次冻结的月池，避免展示最新月池替代运行实际输入。
func poolsOf(pools map[string]strategy.PoolVersion) map[string]any {
	out := make(map[string]any, len(pools))
	for month, pool := range pools {
		entries := make([]map[string]any, 0, len(pool.Entries))
		for _, e := range pool.Entries {
			entries = append(entries, map[string]any{"rank": e.Rank, "ts_code": e.TsCode, "amount": e.Amount})
		}
		out[month] = map[string]any{"month": month, "range_from": pool.RangeFrom,
			"range_to": pool.RangeTo, "dataset_v": pool.DatasetV, "entries": entries}
	}
	return out
}

// backtestReq 回测请求体，对齐 contracts BacktestReq。
type backtestReq struct {
	DataPolicy   string                  `json:"data_policy"`
	Account      *strategy.AccountConfig `json:"account"`
	Params       *strategy.StrategyParams `json:"params"`
	Version      string                  `json:"version"`
	EffectMonths []string                `json:"effect_months"`
	From         string                  `json:"from"`
	To           string                  `json:"to"`
}

// accountConfig 返回请求的有效账户，未提供时明确使用默认值。
func (r backtestReq) accountConfig() strategy.AccountConfig {
	if r.Account == nil {
		return strategy.DefaultAccount()
	}
	return *r.Account
}

// paramsConfig 返回请求的有效参数，未提供或零值时使用默认值。
func (r backtestReq) paramsConfig() strategy.StrategyParams {
	p := strategy.DefaultParams()
	if r.Params == nil {
		return p
	}
	if r.Params.MaFast > 0 { p.MaFast = r.Params.MaFast }
	if r.Params.MaSlow > 0 { p.MaSlow = r.Params.MaSlow }
	if r.Params.AtrN > 0 { p.AtrN = r.Params.AtrN }
	if r.Params.MomentumN > 0 { p.MomentumN = r.Params.MomentumN }
	if r.Params.SlopeN > 0 { p.SlopeN = r.Params.SlopeN }
	if r.Params.BreakoutN > 0 { p.BreakoutN = r.Params.BreakoutN }
	if r.Params.AmountN > 0 { p.AmountN = r.Params.AmountN }
	if r.Params.AmountLow > 0 { p.AmountLow = r.Params.AmountLow }
	if r.Params.AmountHigh > 0 { p.AmountHigh = r.Params.AmountHigh }
	if r.Params.MaxExtension > 0 { p.MaxExtension = r.Params.MaxExtension }
	if r.Params.InitStopAtr > 0 { p.InitStopAtr = r.Params.InitStopAtr }
	if r.Params.TrailAtr > 0 { p.TrailAtr = r.Params.TrailAtr }
	if r.Params.MaExitDays > 0 { p.MaExitDays = r.Params.MaExitDays }
	if r.Params.CooldownDays > 0 { p.CooldownDays = r.Params.CooldownDays }
	if r.Params.GapUpLimit > 0 { p.GapUpLimit = r.Params.GapUpLimit }
	if r.Params.Commission > 0 { p.Commission = r.Params.Commission }
	if r.Params.MinCommission > 0 { p.MinCommission = r.Params.MinCommission }
	if r.Params.SlippageBps > 0 { p.SlippageBps = r.Params.SlippageBps }
	if r.Params.StampRate > 0 { p.StampRate = r.Params.StampRate }
	return p
}

// validVersions 允许的研究版本；S0仅为基准，未完成的S3停止接受新任务。
var validVersions = map[string]bool{"S0": true, "S1": true, "S2": true, "F1": true}

// validMonth 校验 YYYY-MM。
func validMonth(m string) bool {
	if len(m) != 7 || m[4] != '-' {
		return false
	}
	_, err := time.Parse("2006-01", m)
	return err == nil
}

// validDay 校验 YYYY-MM-DD。
func validDay(d string) bool {
	if len(d) != 10 {
		return false
	}
	_, err := time.Parse("2006-01-02", d)
	return err == nil
}

// handleRunBacktest 校验参数后只入队，计算由后台消费者执行。
func (s *Server) handleRunBacktest(w http.ResponseWriter, r *http.Request) {
	account := strategy.DefaultAccount()
	req := backtestReq{Account: &account, DataPolicy: "strict"}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "请求体非法"})
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "请求必须包含单个JSON对象"})
		return
	}
	if req.DataPolicy != "strict" && req.DataPolicy != "available_candidates" {
		writeJSON(w, 400, map[string]string{"detail": "data_policy只允许strict或available_candidates"})
		return
	}
	account = req.accountConfig()
	if err := strategy.ValidateAccount(account); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": err.Error()})
		return
	}
	params := req.paramsConfig()
	if err := strategy.ValidateParams(params); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": err.Error()})
		return
	}
	req.Version = strings.ToUpper(strings.TrimSpace(req.Version))
	if !validVersions[req.Version] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "version 只允许 S0（研究基准）/S1/S2/F1；S3 已停用"})
		return
	}
	if len(req.EffectMonths) < 1 || len(req.EffectMonths) > 13 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "effect_months 只允许 1~13 个月份"})
		return
	}
	for _, m := range req.EffectMonths {
		if !validMonth(m) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "月份须为 YYYY-MM: " + m})
			return
		}
	}
	if !validDay(req.From) || !validDay(req.To) || req.From > req.To {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "from/to 须为合法区间 YYYY-MM-DD"})
		return
	}
	from, _ := time.Parse("2006-01-02", req.From)
	to, _ := time.Parse("2006-01-02", req.To)
	if to.Sub(from).Hours()/24 > 366 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "回放跨度不得超过 366 天"})
		return
	}
	sort.Strings(req.EffectMonths)
	expected := []string{}
	for d := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, from.Location()); !d.After(to); d = d.AddDate(0, 1, 0) {
		expected = append(expected, d.Format("2006-01"))
	}
	if strings.Join(expected, ",") != strings.Join(req.EffectMonths, ",") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "effect_months须覆盖起止区间全部月份且不重复"})
		return
	}
	req.Account = &account
	req.Params = &params
	s.enqueueBacktest(w, req)
}

// intentsOf 未执行意图装配：方向/代码/股数/信号日/参考价/理由。
func intentsOf(list []strategy.Intent) []map[string]any {
	out := make([]map[string]any, 0, len(list))
	for _, it := range list {
		out = append(out, map[string]any{
			"side": it.Side, "ts_code": it.TsCode, "shares": it.Shares,
			"signal_date": it.SignalDate, "ref_price": sanitize(it.RefPrice),
			"stop_price": sanitize(it.StopPrice), "reason": string(it.Reason), "market_cap": sanitize(it.MarketCap),
		})
	}
	return out
}

// s0Response S0 结果装配：期末净值、收益、最大回撤、漏斗、序列与未执行意图。
func s0Response(req backtestReq, res strategy.S0Result) map[string]any {
	series := make([]map[string]any, 0, len(res.Series))
	maxDD := 0.0
	for _, p := range res.Series {
		if p.Drawdown > maxDD {
			maxDD = p.Drawdown
		}
		series = append(series, map[string]any{
			"date": p.Date, "nav": sanitize(p.NAV), "drawdown": sanitize(p.Drawdown),
			"held": p.Held, "cash_rate": sanitize(p.CashRate),
		})
	}
	cycles, summary := res.Ledger.TradeCycles()
	account := req.accountConfig()
	final := account.InitialCapital
	if len(res.Series) > 0 {
		final = res.Series[len(res.Series)-1].NAV
	}
	return map[string]any{
		"version": req.Version, "effect_months": req.EffectMonths, "account": account,
		"params": req.paramsConfig(),
		"points": len(res.Series), "trades": res.Trades, "halted": res.Halted,
		"final_nav": sanitize(final), "total_return": sanitize(final/account.InitialCapital - 1),
		"max_drawdown": sanitize(maxDD),
		"funnel": map[string]any{
			"raw20": res.Funnel.Raw20, "tradable": res.Funnel.Tradable,
			"market_pass": res.Funnel.MarketPass, "signal_hit": res.Funnel.SignalHit,
			"filled": res.Funnel.Filled, "no_cash": res.Funnel.NoCash, "rejected": res.Funnel.Rejected,
		},
		"cycles": cycles, "trade_summary": summary,
		"fills":             ledgerFills(res.Ledger),
		"corporate_actions": corporateActionsOf(res.Ledger.CorporateActions),
		"dividends_received": sanitize(res.Ledger.DividendsReceived),
		"final_cash":        res.Ledger.Cash, "fees_paid": res.Ledger.FeesPaid,
		"closing_positions": closingPositions(res.Ledger),
		"series":            series,
		"open_intents":      intentsOf(res.OpenIntents),
	}
}

// s12Response S1/S2/F1 结果装配（序列结构与 S0 一致）。
func s12Response(req backtestReq, res strategy.S12Result) map[string]any {
	series := make([]map[string]any, 0, len(res.Series))
	maxDD := 0.0
	for _, p := range res.Series {
		if p.Drawdown > maxDD {
			maxDD = p.Drawdown
		}
		series = append(series, map[string]any{
			"date": p.Date, "nav": sanitize(p.NAV), "drawdown": sanitize(p.Drawdown),
			"held": p.Held, "cash_rate": sanitize(p.CashRate),
		})
	}
	cycles, summary := res.Ledger.TradeCycles()
	account := req.accountConfig()
	final := account.InitialCapital
	if len(res.Series) > 0 {
		final = res.Series[len(res.Series)-1].NAV
	}
	return map[string]any{
		"version": req.Version, "effect_months": req.EffectMonths, "account": account,
		"params": req.paramsConfig(),
		"points": len(res.Series), "trades": res.Trades, "halted": res.Halted,
		"final_nav": sanitize(final), "total_return": sanitize(final/account.InitialCapital - 1),
		"max_drawdown": sanitize(maxDD),
		"funnel": map[string]any{
			"raw20": res.Funnel.Raw20, "tradable": res.Funnel.Tradable,
			"market_pass": res.Funnel.MarketPass, "signal_hit": res.Funnel.SignalHit,
			"filled": res.Funnel.Filled, "no_cash": res.Funnel.NoCash, "rejected": res.Funnel.Rejected,
		},
		"cycles": cycles, "trade_summary": summary,
		"fills":             ledgerFills(res.Ledger),
		"corporate_actions": corporateActionsOf(res.Ledger.CorporateActions),
		"dividends_received": sanitize(res.Ledger.DividendsReceived),
		"final_cash":        res.Ledger.Cash, "fees_paid": res.Ledger.FeesPaid,
		"closing_positions": closingPositions(res.Ledger),
		"series":            series,
		"open_intents":      intentsOf(res.OpenIntents),
	}
}

// handleGetPool 月份快照查询，无快照 404。
func (s *Server) handleGetPool(w http.ResponseWriter, r *http.Request) {
	month := strings.TrimSpace(r.URL.Query().Get("month"))
	if !validMonth(month) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "month 须为 YYYY-MM"})
		return
	}
	pool, ok, err := s.Engine.LoadPoolMonth(month)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "查询失败"})
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "无该月快照，回测运行后自动生成"})
		return
	}
	entries := make([]map[string]any, 0, len(pool.Entries))
	for _, e := range pool.Entries {
		entries = append(entries, map[string]any{
			"rank": e.Rank, "ts_code": e.TsCode, "amount": sanitize(e.Amount),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"month": pool.Month, "range_from": pool.RangeFrom,
		"range_to": pool.RangeTo, "dataset_v": pool.DatasetV, "entries": entries,
	})
}

// ledgerFills 空成交返回数组，旧保存结果不补写。
func ledgerFills(l *strategy.Ledger) []strategy.TradeFill {
	if l.Fills == nil {
		return []strategy.TradeFill{}
	}
	return l.Fills
}

// closingPositions 按证券排序返回期末持股，均价不含买入佣金。
func closingPositions(l *strategy.Ledger) []map[string]any {
	codes := make([]string, 0, len(l.Holdings))
	for code := range l.Holdings {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	out := make([]map[string]any, 0, len(codes))
	for _, code := range codes {
		h := l.Holdings[code]
		out = append(out, map[string]any{"ts_code": code, "shares": h.Shares, "avg_cost": h.AvgCost, "stop": h.Stop})
	}
	return out
}

// corporateActionsOf 空公司行动返回空数组。
func corporateActionsOf(ca []strategy.CorporateActionFill) []strategy.CorporateActionFill {
	if ca == nil {
		return []strategy.CorporateActionFill{}
	}
	return ca
}
