// Datasets 受控原始采集数据只读接口。
package api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// dataset 数据集白名单配置。
type dataset struct {
	// Desc 业务描述。
	Desc string
	// DateCol 日期字段，无则为空。
	DateCol string
	// Excluded 不暴露字段。
	Excluded []string
}

// datasets 白名单 20 表，对齐旧 datasets.py。
var datasets = map[string]dataset{
	"stock_basic":            {Desc: "股票基础信息", DateCol: "list_date", Excluded: []string{"pinyin", "pinyin_abbr"}},
	"daily_price":            {Desc: "股票日线行情", DateCol: "trade_date", Excluded: []string{"factors"}},
	"stock_concepts":         {Desc: "概念分类"},
	"stock_concept_details":  {Desc: "概念成分"},
	"stock_moneyflow":        {Desc: "个股资金流向", DateCol: "trade_date", Excluded: []string{"net_mf_ratio"}},
	"market_index":           {Desc: "市场指数日线", DateCol: "trade_date"},
	"stock_margin":           {Desc: "融资融券明细", DateCol: "trade_date"},
	"stock_income":           {Desc: "利润表", DateCol: "end_date"},
	"stock_balance_sheet":    {Desc: "资产负债表", DateCol: "end_date"},
	"stock_cashflow":         {Desc: "现金流量表", DateCol: "end_date"},
	"stock_fina_indicator":   {Desc: "财务指标", DateCol: "end_date"},
	"stock_daily_basic":      {Desc: "日频基础指标", DateCol: "trade_date"},
	"stock_index_member_all": {Desc: "申万行业成分", DateCol: "in_date"},
	"stock_express":          {Desc: "业绩快报", DateCol: "end_date"},
	"stock_suspend":          {Desc: "每日停复牌信息", DateCol: "trade_date"},
	"stock_namechange":       {Desc: "股票更名历史", DateCol: "start_date"},
	"stock_dividend":         {Desc: "分红送股数据", DateCol: "end_date"},
	"index_weight":           {Desc: "指数成分及权重", DateCol: "trade_date"},
	"stock_forecast":         {Desc: "业绩预告", DateCol: "end_date"},
	"moneyflow_hsgt":         {Desc: "沪深港通资金流向", DateCol: "trade_date"},
	"strategy_pool_month":    {Desc: "月度Top20股票池快照", DateCol: "effect_month"},
	"strategy_pool_entry":    {Desc: "月度Top20股票池明细", DateCol: "effect_month"},
}

// column 字段定义。
type column struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Nullable   bool   `json:"nullable"`
	PrimaryKey bool   `json:"primary_key"`
}

// queryReq 查询请求体。
type queryReq struct {
	Dataset    string         `json:"dataset"`
	Columns    []string       `json:"columns"`
	Filters    map[string]any `json:"filters"`
	StartDate  string         `json:"start_date"`
	EndDate    string         `json:"end_date"`
	OrderBy    string         `json:"order_by"`
	Descending bool           `json:"descending"`
	Limit      int            `json:"limit"`
	Offset     int            `json:"offset"`
}

// columnsOf 读表字段并剔除黑名单。
func (s *Server) columnsOf(name string) ([]column, error) {
	cfg, ok := datasets[name]
	if !ok {
		return nil, fmt.Errorf("不允许读取数据集: %s", name)
	}
	rows, err := s.DB.QueryRows(fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(name)))
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("数据表尚未初始化: %s", name)
	}
	excluded := map[string]bool{}
	for _, c := range cfg.Excluded {
		excluded[c] = true
	}
	out := make([]column, 0, len(rows))
	for _, r := range rows {
		nm := fmt.Sprint(r["name"])
		if excluded[nm] {
			continue
		}
		out = append(out, column{
			Name:       nm,
			Type:       fmt.Sprint(r["type"]),
			Nullable:   fmt.Sprint(r["notnull"]) == "0",
			PrimaryKey: fmt.Sprint(r["pk"]) != "0",
		})
	}
	return out, nil
}

// quoteIdent 双引号转义标识符。
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// handleListDatasets 列出白名单数据集。
func (s *Server) handleListDatasets(w http.ResponseWriter, _ *http.Request) {
	names := make([]string, 0, len(datasets))
	for n := range datasets {
		names = append(names, n)
	}
	sort.Strings(names)
	list := make([]map[string]string, 0, len(names))
	for _, n := range names {
		list = append(list, map[string]string{
			"name": n, "description": datasets[n].Desc, "date_column": datasets[n].DateCol,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"datasets": list})
}

// handleDescribeDataset 单表字段定义。
func (s *Server) handleDescribeDataset(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cfg, ok := datasets[name]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "不允许读取数据集: " + name})
		return
	}
	cols, err := s.columnsOf(name)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name": name, "description": cfg.Desc, "date_column": cfg.DateCol, "columns": cols,
	})
}

// handleQueryDataset 参数化限量只读查询。
func (s *Server) handleQueryDataset(w http.ResponseWriter, r *http.Request) {
	var req queryReq
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "请求体非法"})
		return
	}
	cfg, ok := datasets[req.Dataset]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "不允许读取数据集: " + req.Dataset})
		return
	}
	cols, err := s.columnsOf(req.Dataset)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": err.Error()})
		return
	}
	available := map[string]bool{}
	for _, c := range cols {
		available[c.Name] = true
	}
	selected := req.Columns
	if len(selected) == 0 {
		for _, c := range cols {
			selected = append(selected, c.Name)
		}
	}
	var bad []string
	for _, c := range selected {
		if !available[c] {
			bad = append(bad, c)
		}
	}
	if len(bad) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "未知字段: " + strings.Join(bad, ", ")})
		return
	}
	bad = bad[:0]
	for k := range req.Filters {
		if !available[k] {
			bad = append(bad, k)
		}
	}
	if len(bad) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "未知过滤字段: " + strings.Join(bad, ", ")})
		return
	}
	if (req.StartDate != "" || req.EndDate != "") && cfg.DateCol == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "该数据集没有日期范围字段"})
		return
	}
	orderBy := req.OrderBy
	if orderBy == "" {
		orderBy = cfg.DateCol
		if orderBy == "" {
			orderBy = selected[0]
		}
	}
	if !available[orderBy] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "未知排序字段: " + orderBy})
		return
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	if offset > 1000000 {
		offset = 1000000
	}

	var where []string
	var args []any
	for k, v := range req.Filters {
		if v == nil {
			where = append(where, quoteIdent(k)+" IS NULL")
			continue
		}
		where = append(where, quoteIdent(k)+" = ?")
		args = append(args, v)
	}
	if req.StartDate != "" {
		where = append(where, quoteIdent(cfg.DateCol)+" >= ?")
		args = append(args, req.StartDate)
	}
	if req.EndDate != "" {
		where = append(where, quoteIdent(cfg.DateCol)+" <= ?")
		args = append(args, req.EndDate)
	}
	sel := make([]string, len(selected))
	for i, c := range selected {
		sel[i] = quoteIdent(c)
	}
	sql := "SELECT " + strings.Join(sel, ", ") + " FROM " + quoteIdent(req.Dataset)
	if len(where) > 0 {
		sql += " WHERE " + strings.Join(where, " AND ")
	}
	dir := "ASC"
	if req.Descending {
		dir = "DESC"
	}
	sql += " ORDER BY " + quoteIdent(orderBy) + " " + dir + " LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := s.DB.QueryRows(sql, args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "查询失败"})
		return
	}
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		m := make(map[string]any, len(selected))
		for _, c := range selected {
			m[c] = sanitize(r[c])
		}
		out = append(out, m)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"dataset": req.Dataset, "columns": selected, "rows": out,
		"count": len(out), "limit": limit, "offset": offset,
	})
}
