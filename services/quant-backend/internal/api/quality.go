// Quality 数据覆盖治理接口：截至何时、是否完整、缺什么、为何缺、何时补。
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"quantbackend/internal/calendar"
)

// datasetMeta 数据集治理元信息：归属任务、达标线、补采提示。
type datasetMeta struct {
	// Task 归属 ETL 任务名（params_json 中的 task）。
	Task string
	// Ratio 最新日计数达标线（相对上市股票池），事件表用 -1 表示按日期存在判定。
	Ratio float64
	// Expect 固定期望行数（>0 时优先，如 market_index 每日 5 条）。
	Expect int64
	// Dim 维度表：只看非空，不看日期。
	Dim bool
	// Event 事件表：按 max 日期新鲜度（窗口天数）判定，不按最新交易日对齐。
	Event bool
	// Window 事件表新鲜度窗口天数。
	Window int
	// Quarterly 季度表：按期望季度缺口判定（覆盖率<95% 的季度进 missing）。
	Quarterly bool
	// Next 下次补采提示。
	Next string
}

// qualityMeta 20 表治理元信息。
var qualityMeta = map[string]datasetMeta{
	"stock_basic":            {Task: "basic", Ratio: -1, Dim: true, Next: "每日00:05自动"},
	"daily_price":            {Task: "price", Ratio: 0.9, Next: "每日16:45自动"},
	"stock_concepts":         {Task: "concepts", Ratio: -1, Dim: true, Next: "手动 concepts"},
	"stock_concept_details":  {Task: "concepts", Ratio: -1, Dim: true, Next: "手动 concepts"},
	"stock_moneyflow":        {Task: "moneyflow", Ratio: 0.8, Next: "每日16:45自动（含 daily_update）"},
	"market_index":           {Task: "index", Ratio: -1, Expect: 5, Next: "每日16:45自动"},
	"stock_margin":           {Task: "margin", Ratio: 0.75, Next: "每日19:07自动"},
	"stock_income":           {Task: "income", Ratio: 0.95, Quarterly: true, Next: "每周日02:00自动"},
	"stock_balance_sheet":    {Task: "balancesheet", Ratio: 0.95, Quarterly: true, Next: "每周日02:00自动"},
	"stock_cashflow":         {Task: "cashflow", Ratio: 0.95, Quarterly: true, Next: "每周日02:00自动"},
	"stock_fina_indicator":   {Task: "fina_indicator", Ratio: 0.95, Quarterly: true, Next: "手动 fina_indicator"},
	"stock_daily_basic":      {Task: "daily_basic", Ratio: 0.9, Next: "每日16:45自动"},
	"stock_index_member_all": {Task: "index_members", Ratio: -1, Dim: true, Next: "手动 index_members"},
	"stock_express":          {Task: "express", Ratio: -1, Event: true, Window: 120, Next: "每日16:45自动"},
	"stock_suspend":          {Task: "suspend", Ratio: -1, Next: "每日16:45自动"},
	"stock_namechange":       {Task: "namechange", Ratio: -1, Event: true, Window: 365, Next: "手动 namechange"},
	"stock_dividend":         {Task: "dividend", Ratio: -1, Event: true, Window: 120, Next: "手动 dividend"},
	"index_weight":           {Task: "index_weight", Ratio: -1, Next: "每日16:45自动"},
	"stock_forecast":         {Task: "forecast", Ratio: -1, Event: true, Window: 120, Next: "每日16:45自动"},
	"moneyflow_hsgt":         {Task: "hsgt", Ratio: -1, Expect: 1, Next: "每日16:45自动"},
}

// tableQuality 单表质量快照。
type tableQuality struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	MinDate     string     `json:"min_date"`
	MaxDate     string     `json:"max_date"`
	Total       int64      `json:"total"`
	Recent      []dayCount `json:"recent"`
	AsOf        string     `json:"as_of"`
	Complete    bool       `json:"complete"`
	Ratio       float64    `json:"ratio"`
	Missing     []string   `json:"missing"`
	Reason      string     `json:"reason"`
	Next        string     `json:"next"`
}

// dayCount 单日计数。
type dayCount struct {
	Date  string `json:"date"`
	Count int64  `json:"count"`
}

// handleQualityCoverage 全库质量快照，用于质量看板与补采决策（带 20s 内存缓存防锁争用）。
func (s *Server) handleQualityCoverage(w http.ResponseWriter, _ *http.Request) {
	s.qualityMu.RLock()
	if len(s.qualityBuf) > 0 && time.Since(s.qualityAt) < 20*time.Second {
		buf := s.qualityBuf
		s.qualityMu.RUnlock()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buf)
		return
	}
	s.qualityMu.RUnlock()

	universe, _ := s.DB.QueryInt("SELECT COUNT(*) FROM stock_basic WHERE list_status='L'")
	if universe <= 0 {
		universe = 5000
	}
	cal := calendar.New(s.DB)
	latestTrade := ""
	if days := cal.TradingDays(dateShiftDays(cal.Today(), -15), cal.Today()); len(days) > 0 {
		latestTrade = days[len(days)-1]
	}
	names := make([]string, 0, len(datasets))
	for n := range datasets {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]tableQuality, 0, len(names))
	for _, n := range names {
		out = append(out, s.tableQuality(n, datasets[n], qualityMeta[n], universe, cal, latestTrade))
	}
	payload, err := json.Marshal(map[string]any{
		"universe": universe, "latest_trading_day": latestTrade, "tables": out,
	})
	if err == nil {
		s.qualityMu.Lock()
		s.qualityBuf = payload
		s.qualityAt = time.Now()
		s.qualityMu.Unlock()
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

// tableQuality 单表质量快照组装。
func (s *Server) tableQuality(name string, cfg dataset, meta datasetMeta,
	universe int64, cal *calendar.Calendar, latestTrade string) tableQuality {
	tq := tableQuality{Name: name, Description: cfg.Desc, Next: meta.Next, Missing: []string{}}
	total, err := s.DB.QueryInt("SELECT COUNT(*) FROM " + quoteIdent(name))
	if err != nil {
		tq.Reason = "查询失败"
		return tq
	}
	tq.Total = total
	tq.Reason = s.lastTaskReason(meta.Task)
	if cfg.DateCol == "" || meta.Dim {
		tq.Complete = total > 0
		tq.AsOf = ""
		if total == 0 {
			tq.Reason = "表空，" + tq.Reason
		}
		return tq
	}
	dc := quoteIdent(cfg.DateCol)
	rows, err := s.DB.QueryRows("SELECT MIN(" + dc + ") AS mn, MAX(" + dc +
		") AS mx FROM " + quoteIdent(name))
	if err != nil || len(rows) == 0 {
		return tq
	}
	tq.MinDate, tq.MaxDate = strOf(rows[0]["mn"]), strOf(rows[0]["mx"])
	recent, _ := s.DB.QueryRows("SELECT " + dc + " AS d, COUNT(*) AS c FROM " +
		quoteIdent(name) + " GROUP BY " + dc + " ORDER BY " + dc + " DESC LIMIT 10")
	byDate := map[string]int64{}
	for _, r := range recent {
		c := int64(intOf(r["c"]))
		byDate[strOf(r["d"])] = c
		tq.Recent = append(tq.Recent, dayCount{Date: strOf(r["d"]), Count: c})
	}
	if meta.Expect > 0 {
		// 固定期望行（如指数每日 5 条、hsgt 每日 1 条）。
		tq.AsOf, tq.Complete = "", false
		for _, r := range tq.Recent {
			if r.Count >= meta.Expect {
				if tq.AsOf == "" {
					tq.AsOf = r.Date
				}
				continue
			}
			tq.Missing = append(tq.Missing, r.Date)
		}
		tq.Complete = tq.AsOf == latestTrade
		tq.Ratio = 1
		return tq
	}
	if meta.Ratio < 0 {
		if meta.Event {
			// 事件表：max 日期在窗口内即新鲜，不对齐最新交易日。
			tq.AsOf = tq.MaxDate
			win := meta.Window
			if win <= 0 {
				win = 120
			}
			tq.Complete = withinDays(tq.MaxDate, latestTrade, win)
			return tq
		}
		// 每日事件表（停复牌/权重）：按最新交易日是否存在判定是否同步到位。
		tq.AsOf = tq.MaxDate
		tq.Complete = tq.MaxDate == latestTrade
		if !tq.Complete && latestTrade != "" {
			tq.Missing = []string{latestTrade}
		}
		return tq
	}
	// 计数表：达标线判定，找最新完整日。
	expect := int64(float64(universe) * meta.Ratio)
	for _, r := range tq.Recent {
		if r.Count >= expect {
			if tq.AsOf == "" {
				tq.AsOf = r.Date
				tq.Ratio = float64(r.Count) / float64(universe)
			}
			continue
		}
		tq.Missing = append(tq.Missing, r.Date)
	}
	if meta.Quarterly {
		// 季度表：补上“零行季度”（GROUP BY 看不见的真缺口）。
		tq.Missing = s.quarterGaps(name, universe, meta.Ratio)
	}
	tq.Complete = tq.AsOf == latestTrade && latestTrade != ""
	if meta.Quarterly {
		// 季度表完整 = 最近期望季度达标。
		tq.Complete = len(tq.Missing) == 0
	}
	if !tq.Complete && tq.Reason == "" {
		tq.Reason = "未达标或未同步"
	}
	return tq
}

// quarterGaps 期望季度中覆盖率不足的季度（近 6 个，分母为当时已上市）。
func (s *Server) quarterGaps(table string, universe int64, ratio float64) []string {
	var missing []string
	for _, q := range lastQuarterEnds(6) {
		denom := s.quarterUniverse(q, universe)
		n, err := s.DB.QueryInt("SELECT COUNT(DISTINCT ts_code) FROM "+quoteIdent(table)+
			" WHERE end_date=?", q)
		if err != nil || n < int64(float64(denom)*ratio) {
			missing = append(missing, q)
		}
	}
	return missing
}

// quarterUniverse 某季度末时已上市股票数，查不到回退当前全池。
func (s *Server) quarterUniverse(quarterEnd string, fallback int64) int64 {
	n, err := s.DB.QueryInt(`SELECT COUNT(*) FROM stock_basic
		WHERE list_status='L' AND list_date <= ?`, quarterEnd)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

// lastQuarterEnds 最近 n 个季度末 YYYY-MM-DD（倒序）。
func lastQuarterEnds(n int) []string {
	now := time.Now()
	y, m := now.Year(), int(now.Month())
	ends := []struct {
		month int
		day   string
	}{{12, "12-31"}, {9, "09-30"}, {6, "06-30"}, {3, "03-31"}}
	need := map[int]int{3: 4, 6: 8, 9: 11, 12: 13}
	var out []string
	for len(out) < n {
		for _, q := range ends {
			if y == now.Year() && m < need[q.month] {
				continue
			}
			out = append(out, fmt.Sprintf("%04d-%s", y, q.day))
			if len(out) >= n {
				break
			}
		}
		y--
	}
	return out
}

// withinDays 日期 d 是否新鲜：不晚于 ref 且在 ref 往前 window 天内；
// 未来的 max 日期（如远期预告）视为新鲜。
func withinDays(d, ref string, window int) bool {
	td, err1 := parseDay(d)
	tr, err2 := parseDay(ref)
	if err1 != nil || err2 != nil {
		return false
	}
	diff := tr.Sub(td)
	return diff <= time.Duration(window)*24*time.Hour &&
		diff >= -400*24*time.Hour
}

// taskPattern 归属任务匹配模式。注意 raw 字符串 `"%"` 为 3 字符，
// 此处尾部只要 quote+percent，须写作 "`+`%"` 形式，禁止多写一个引号。
func taskPattern(task string) string {
	return `%"task":"` + task + `"%`
}

// lastTaskReason 归属任务最近一次状态与错误摘要。
func (s *Server) lastTaskReason(task string) string {
	rows, err := s.DB.QueryRows(`SELECT status, error, finished_at, created_at FROM etl_tasks
		WHERE params_json LIKE ? ORDER BY created_at DESC LIMIT 1`, taskPattern(task))
	if err != nil || len(rows) == 0 {
		return "无执行记录"
	}
	r := rows[0]
	st := strOf(r["status"])
	ts := strOf(r["finished_at"])
	if ts == "" {
		ts = strOf(r["created_at"])
	}
	if e := strOf(r["error"]); e != "" {
		if len(e) > 80 {
			e = e[:80]
		}
		return fmt.Sprintf("最近%s(%s)：%s", st, ts, e)
	}
	return fmt.Sprintf("最近%s(%s)", st, ts)
}

// dateShiftDays 日期偏移 N 天（YYYY-MM-DD）。
func dateShiftDays(day string, n int) string {
	t, err := parseDay(day)
	if err != nil {
		return day
	}
	return t.AddDate(0, 0, n).Format("2006-01-02")
}

// parseDay 解析 YYYY-MM-DD（容忍 YYYYMMDD）。
func parseDay(day string) (t time.Time, err error) {
	d := strings.ReplaceAll(day, "-", "")
	if len(d) == 8 {
		d = d[:4] + "-" + d[4:6] + "-" + d[6:8]
	}
	return time.Parse("2006-01-02", d)
}
