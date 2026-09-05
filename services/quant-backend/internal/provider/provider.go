// Package provider Tushare 数据源 HTTP 直调客户端。
//
// Tushare pro API 即 HTTP POST，请求体为 api_name/token/params/fields，
// 响应 data 段为 fields + items 二维数组，本包将其转为 []db.Row。
package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"quantbackend/internal/db"
)

// Row 行别名，复用 db.Row。
type Row = db.Row

// Provider Tushare 直调客户端，内建限流与重试。
type Provider struct {
	token   string
	isShort bool
	baseURL string
	client  *http.Client
	mu      sync.Mutex
	// lastCall 上次调用时间（长 token 限速用）。
	lastCall time.Time
	// limited 接口名 → 耗尽日期（YYYY-MM-DD，上海），按日失效。
	limited map[string]string
}

// PermissionError 权限不足哨兵错误，调用方可据此记 SKIPPED 而非失败。
type PermissionError struct {
	// API 接口名。
	API string
	// Msg 原始信息。
	Msg string
}

// Error 实现 error。
func (e *PermissionError) Error() string { return "tushare " + e.API + " 权限不足: " + e.Msg }

// todayShanghai 上海今天 YYYY-MM-DD。
func todayShanghai() string {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		loc = time.FixedZone("CST", 8*3600)
	}
	return time.Now().In(loc).Format("2006-01-02")
}

// New 构造客户端，可选地址覆盖默认路由并采用通用采集/限速模式。
func New(tokenType, shortToken, longToken string, apiURL ...string) *Provider {
	token, isShort := longToken, false
	base := "https://api.waditu.com"
	if tokenType == "short" {
		token, isShort = shortToken, true
		base = "http://lianghua.nanyangqiankun.top"
	}
	if len(apiURL) > 0 && strings.TrimSpace(apiURL[0]) != "" {
		base = strings.TrimSpace(apiURL[0])
		isShort = (tokenType == "short")
	}
	return &Provider{
		token:   strings.TrimSpace(token),
		isShort: isShort,
		baseURL: base,
		client:  &http.Client{Timeout: 60 * time.Second},
		limited: map[string]string{},
	}
}

// IsShort 是否使用未覆盖地址的历史专线模式。
func (p *Provider) IsShort() bool { return p.isShort }

// cleanDate 去横杠兼容 tushare 日期格式。
func cleanDate(s string) string { return strings.ReplaceAll(s, "-", "") }

// call 调用单个 tushare 接口，返回行切片。
//
// 语义：日限额耗尽记到上海日期，次日自动恢复，跳过返回 nil,nil；
// 分钟级限流只重试不标记；权限不足返回 PermissionError；空数据返回 nil,nil。
func (p *Provider) call(api string, params map[string]string, fields string) ([]Row, error) {
	p.mu.Lock()
	if day, marked := p.limited[api]; marked && day == todayShanghai() {
		p.mu.Unlock()
		log.Printf("tushare 接口 %s 今日限额已用完，跳过", api)
		return nil, nil
	}
	if el := time.Since(p.lastCall); el < 400*time.Millisecond {
		wait := 400*time.Millisecond - el
		p.mu.Unlock()
		time.Sleep(wait)
		p.mu.Lock()
	}
	token := p.token
	p.mu.Unlock()
	if token == "" {
		return nil, fmt.Errorf("未配置 tushare token，无法执行数据采集")
	}

	bodyMap := map[string]any{"api_name": api, "token": token, "params": params}
	if fields != "" {
		bodyMap["fields"] = fields
	}
	body, _ := json.Marshal(bodyMap)
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		rows, kind, err := p.doCall(api, body)
		if err == nil {
			p.mu.Lock()
			p.lastCall = time.Now()
			p.mu.Unlock()
			return rows, nil
		}
		lastErr = err
		switch kind {
		case errQuota:
			p.mu.Lock()
			p.limited[api] = todayShanghai()
			p.mu.Unlock()
			log.Printf("tushare 接口 %s 今日限额已用完，明日恢复: %.50s", api, err.Error())
			return nil, nil
		case errPermission:
			return nil, &PermissionError{API: api, Msg: firstLine(err.Error())}
		case errThrottle:
			if p.isShort {
				return nil, err
			}
			log.Printf("tushare 限流等待中 %s (尝试 %d/3)", api, attempt+1)
			time.Sleep(time.Duration(5*(attempt+1)) * time.Second)
		default:
			return nil, err
		}
	}
	return nil, lastErr
}

// errKind 错误分类。
type errKind int

const (
	// errThrottle 分钟级限流，可重试。
	errThrottle errKind = iota
	// errQuota 日限额耗尽，当天跳过。
	errQuota
	// errPermission 无权限，记 SKIPPED。
	errPermission
	// errFatal 其他错误，直接返回。
	errFatal
)

// doCall 单次 HTTP 请求。
func (p *Provider) doCall(api string, body []byte) ([]Row, errKind, error) {
	req, err := http.NewRequest("POST", p.baseURL, bytes.NewReader(body))
	if err != nil {
		return nil, errFatal, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, errThrottle, err
	}
	defer resp.Body.Close()
	var payload struct {
		Code int `json:"code"`
		Msg  any `json:"msg"`
		Data *struct {
			Fields  []string `json:"fields"`
			Items   [][]any  `json:"items"`
			HasMore bool     `json:"has_more"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, errThrottle, fmt.Errorf("解析 tushare 响应失败: %w", err)
	}
	if payload.Code != 0 {
		msg := fmt.Sprint(payload.Msg)
		return nil, classifyRemoteError(msg), fmt.Errorf("tushare %s 错误: %s", api, msg)
	}
	if payload.Data == nil {
		return nil, errThrottle, nil
	}
	if payload.Data.HasMore && api != "index_member_all" && !strings.HasSuffix(api, "_vip") &&
		!strings.HasPrefix(api, "fina_indicator") {
		return nil, errFatal, fmt.Errorf("tushare %s 返回未完整分页，拒绝接收", api)
	}
	rows := make([]Row, 0, len(payload.Data.Items))
	for _, item := range payload.Data.Items {
		r := make(Row, len(payload.Data.Fields))
		for i, f := range payload.Data.Fields {
			if i < len(item) {
				r[f] = item[i]
			}
		}
		rows = append(rows, r)
	}
	return rows, errThrottle, nil
}

// classifyRemoteError 按信息分类远端错误。
func classifyRemoteError(msg string) errKind {
	if strings.Contains(msg, "每天最多访问") || strings.Contains(msg, "5000次") {
		return errQuota
	}
	if strings.Contains(msg, "无权限") || strings.Contains(msg, "权限") ||
		strings.Contains(strings.ToLower(msg), "auth") {
		return errPermission
	}
	if strings.Contains(msg, "每分钟最多访问") || strings.Contains(msg, "接口过快") ||
		strings.Contains(msg, "频繁") {
		return errThrottle
	}
	m := strings.ToLower(msg)
	if strings.Contains(m, "rate") || strings.Contains(m, "timeout") || strings.Contains(m, "busy") {
		return errThrottle
	}
	return errFatal
}

// firstLine 取首行并截断。
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 120 {
		return s[:120]
	}
	return s
}

// StockBasic 股票基础信息。
func (p *Provider) StockBasic() ([]Row, error) {
	return p.call("stock_basic", map[string]string{"list_status": "L,D,P"},
		"ts_code,symbol,name,area,industry,market,list_date,fullname,enname,"+
			"curr_type,list_status,is_hs,delist_date,exchange,cnspell")
}

// Daily 日线行情，按日期或按股票二选一。
func (p *Provider) Daily(tradeDate, tsCode, startDate, endDate string) ([]Row, error) {
	params := map[string]string{}
	if tradeDate != "" {
		params["trade_date"] = cleanDate(tradeDate)
	} else if tsCode != "" {
		params["ts_code"] = tsCode
		params["start_date"] = cleanDate(startDate)
		params["end_date"] = cleanDate(endDate)
	} else {
		return nil, nil
	}
	return p.call("daily", params, "")
}

// AdjFactor 复权因子。
func (p *Provider) AdjFactor(tradeDate, tsCode, startDate, endDate string) ([]Row, error) {
	params := map[string]string{}
	if tradeDate != "" {
		params["trade_date"] = cleanDate(tradeDate)
	} else if tsCode != "" {
		params["ts_code"] = tsCode
		params["start_date"] = cleanDate(startDate)
		params["end_date"] = cleanDate(endDate)
	} else {
		return nil, nil
	}
	return p.call("adj_factor", params, "")
}

// Moneyflow 个股资金流向（按日期）。
func (p *Provider) Moneyflow(tradeDate string) ([]Row, error) {
	if tradeDate == "" {
		return nil, nil
	}
	return p.call("moneyflow", map[string]string{"trade_date": cleanDate(tradeDate)}, "")
}

// DailyBasic 日频基础指标。
func (p *Provider) DailyBasic(tradeDate, tsCode, startDate, endDate string) ([]Row, error) {
	params := map[string]string{}
	if tradeDate != "" {
		params["trade_date"] = cleanDate(tradeDate)
	}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	return p.call("daily_basic", params, "")
}

// IndexDaily 指数日线。
func (p *Provider) IndexDaily(tsCode, startDate, endDate string) ([]Row, error) {
	return p.call("index_daily", map[string]string{
		"ts_code": tsCode, "start_date": cleanDate(startDate), "end_date": cleanDate(endDate),
	}, "")
}

// Concept 概念分类目录。
func (p *Provider) Concept() ([]Row, error) {
	return p.call("concept", map[string]string{"src": "ts"}, "")
}

// ConceptDetail 概念明细。
func (p *Provider) ConceptDetail(id string) ([]Row, error) {
	return p.call("concept_detail", map[string]string{"id": id}, "")
}

// ThsIndex 同花顺概念指数（需 6000 积分）。
func (p *Provider) ThsIndex(exchange, typ string) ([]Row, error) {
	params := map[string]string{}
	if exchange != "" {
		params["exchange"] = exchange
	}
	if typ != "" {
		params["type"] = typ
	}
	return p.call("ths_index", params, "")
}

// ThsMember 同花顺概念成分。
func (p *Provider) ThsMember(tsCode, conCode string) ([]Row, error) {
	params := map[string]string{}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if conCode != "" {
		params["con_code"] = conCode
	}
	return p.call("ths_member", params, "")
}

// FinaIndicator 财务指标（单股票）。
func (p *Provider) FinaIndicator(tsCode string) ([]Row, error) {
	return p.call("fina_indicator", map[string]string{"ts_code": tsCode}, "")
}

// IndexMemberAll 申万行业成分。
func (p *Provider) IndexMemberAll(isNew string) ([]Row, error) {
	params := map[string]string{"is_new": isNew, "limit": "2000"}
	var all []Row
	seen := map[string]bool{}
	for offset := 0; offset < 100000; offset += 2000 {
		params["offset"] = strconv.Itoa(offset)
		rows, err := p.call("index_member_all", params, "")
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			key := fmt.Sprint(row["ts_code"], "|", row["l3_code"], "|", row["in_date"])
			if seen[key] {
				return nil, fmt.Errorf("申万行业分页返回重复键，拒绝部分替换")
			}
			seen[key] = true
		}
		all = append(all, rows...)
		if len(rows) < 2000 {
			return all, nil
		}
	}
	return nil, fmt.Errorf("申万行业分页超过安全上限")
}

// Income 利润表，period 为报告期 YYYYMMDD（指定报告期必须用 period，
// end_date 只是公告截止日，用它会漏掉随后才公告的报告）。
func (p *Provider) Income(tsCode, startDate, endDate, period string) ([]Row, error) {
	params := map[string]string{}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	if period != "" {
		params["period"] = cleanDate(period)
	}
	return p.call("income", params, "")
}

// IncomeVip VIP 利润表（仅短 token 批量，同样支持 period）。
func (p *Provider) IncomeVip(tsCode, startDate, endDate, period string) ([]Row, error) {
	if !p.isShort {
		return nil, nil
	}
	params := map[string]string{"limit": "10000"}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	if period != "" {
		params["period"] = cleanDate(period)
	}
	return p.call("income_vip", params, "")
}

// BalanceSheetVip VIP 资产负债表（仅短 token 批量，支持 period）。
func (p *Provider) BalanceSheetVip(tsCode, startDate, endDate, period string) ([]Row, error) {
	if !p.isShort {
		return nil, nil
	}
	params := map[string]string{"limit": "10000"}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	if period != "" {
		params["period"] = cleanDate(period)
	}
	return p.call("balancesheet_vip", params, "")
}

// CashflowVip VIP 现金流量表（仅短 token 批量，支持 period）。
func (p *Provider) CashflowVip(tsCode, startDate, endDate, period string) ([]Row, error) {
	if !p.isShort {
		return nil, nil
	}
	params := map[string]string{"limit": "10000"}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	if period != "" {
		params["period"] = cleanDate(period)
	}
	return p.call("cashflow_vip", params, "")
}

// FinaIndicatorVip VIP 财务指标（仅短 token 批量，支持 period）。
func (p *Provider) FinaIndicatorVip(tsCode, startDate, endDate, period string) ([]Row, error) {
	if !p.isShort {
		return nil, nil
	}
	params := map[string]string{"limit": "10000"}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	if period != "" {
		params["period"] = cleanDate(period)
	}
	return p.call("fina_indicator_vip", params, "")
}

// BalanceSheet 资产负债表，period 语义同 Income。
func (p *Provider) BalanceSheet(tsCode, startDate, endDate, period string) ([]Row, error) {
	params := map[string]string{"ts_code": tsCode}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	if period != "" {
		params["period"] = cleanDate(period)
	}
	return p.call("balancesheet", params, "")
}

// Cashflow 现金流量表，period 语义同 Income。
func (p *Provider) Cashflow(tsCode, startDate, endDate, period string) ([]Row, error) {
	params := map[string]string{"ts_code": tsCode}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	if period != "" {
		params["period"] = cleanDate(period)
	}
	return p.call("cashflow", params, "")
}

// Express 业绩快报。
func (p *Provider) Express(tsCode, startDate, endDate string) ([]Row, error) {
	params := map[string]string{}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	return p.call("express", params, "")
}

// TradeCal 交易日历。
func (p *Provider) TradeCal(startDate, endDate string) ([]Row, error) {
	params := map[string]string{"exchange": "SSE"}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	return p.call("trade_cal", params, "")
}

// Margin 融资融券汇总。
func (p *Provider) Margin(tradeDate, exchangeID string) ([]Row, error) {
	params := map[string]string{}
	if tradeDate != "" {
		params["trade_date"] = cleanDate(tradeDate)
	}
	if exchangeID != "" {
		params["exchange_id"] = exchangeID
	}
	return p.call("margin", params, "")
}

// MarginDetail 融资融券明细（按股票）。
func (p *Provider) MarginDetail(tradeDate string) ([]Row, error) {
	params := map[string]string{}
	if tradeDate != "" {
		params["trade_date"] = cleanDate(tradeDate)
	}
	return p.call("margin_detail", params, "")
}

// SuspendD 每日停复牌信息。
func (p *Provider) SuspendD(tradeDate, tsCode, startDate, endDate string) ([]Row, error) {
	params := map[string]string{}
	if tradeDate != "" {
		params["trade_date"] = cleanDate(tradeDate)
	}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	return p.call("suspend_d", params, "")
}

// Namechange 股票更名记录。
func (p *Provider) Namechange(tsCode, startDate, endDate string) ([]Row, error) {
	params := map[string]string{}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	return p.call("namechange", params, "")
}

// Dividend 分红送股。
func (p *Provider) Dividend(tsCode, annDate, recordDate, exDate string) ([]Row, error) {
	params := map[string]string{}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if annDate != "" {
		params["ann_date"] = cleanDate(annDate)
	}
	if recordDate != "" {
		params["record_date"] = cleanDate(recordDate)
	}
	if exDate != "" {
		params["ex_date"] = cleanDate(exDate)
	}
	fields := "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax," +
		"record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date,base_share"
	rows, err := p.call("dividend", params, fields)
	if err != nil {
		return nil, err
	}
	if len(rows) >= 2000 {
		return nil, fmt.Errorf("dividend返回达到2000行上限，无法确认数据完整性")
	}
	return rows, nil
}

// IndexWeight 指数成分权重。
func (p *Provider) IndexWeight(indexCode, tradeDate, startDate, endDate string) ([]Row, error) {
	params := map[string]string{}
	if indexCode != "" {
		params["index_code"] = indexCode
	}
	if tradeDate != "" {
		params["trade_date"] = cleanDate(tradeDate)
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	return p.call("index_weight", params, "")
}

// Forecast 业绩预告。
func (p *Provider) Forecast(tsCode, annDate, startDate, endDate, period string) ([]Row, error) {
	params := map[string]string{}
	if tsCode != "" {
		params["ts_code"] = tsCode
	}
	if annDate != "" {
		params["ann_date"] = cleanDate(annDate)
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	if period != "" {
		params["period"] = cleanDate(period)
	}
	return p.call("forecast", params, "")
}

// MoneyflowHsgt 沪深港通资金流向。
func (p *Provider) MoneyflowHsgt(tradeDate, startDate, endDate string) ([]Row, error) {
	params := map[string]string{}
	if tradeDate != "" {
		params["trade_date"] = cleanDate(tradeDate)
	}
	if startDate != "" {
		params["start_date"] = cleanDate(startDate)
	}
	if endDate != "" {
		params["end_date"] = cleanDate(endDate)
	}
	return p.call("moneyflow_hsgt", params, "")
}
