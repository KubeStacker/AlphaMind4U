// Package engine ETL 任务公共小助手：日期归一化与阈值查询。
package engine

import (
	"fmt"
	"log"
	"strings"
	"time"

	"quantbackend/internal/db"
)

// normDate 归一化日期为 YYYY-MM-DD 文本。
func normDate(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		s := strings.TrimSpace(t)
		if s == "" || s == "0000-00-00" || s == "00000000" {
			return ""
		}
		if len(s) >= 10 && s[4] == '-' && s[7] == '-' {
			return s[:10]
		}
		d := strings.ReplaceAll(s, "-", "")
		d = strings.ReplaceAll(d, "/", "")
		if len(d) >= 8 {
			d = d[:8]
			return d[:4] + "-" + d[4:6] + "-" + d[6:8]
		}
		return s
	case time.Time:
		return t.Format("2006-01-02")
	case []byte:
		return normDate(string(t))
	default:
		return normDate(fmt.Sprint(v))
	}
}

// strVal 转字符串，nil 返回空串。
func strVal(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

// normDateFields 批量归一化行内日期字段。
func normDateFields(rows []db.Row, fields ...string) {
	for _, r := range rows {
		for _, f := range fields {
			if v, ok := r[f]; ok && v != nil {
				if s := normDate(v); s != "" {
					r[f] = s
				} else {
					r[f] = nil
				}
			}
		}
	}
}

// sleepNormal 通用接口限速等待约 400ms。
func sleepNormal() { time.Sleep(400 * time.Millisecond) }

// dateShift 解析 YYYY-MM-DD 并按年/天偏移。
func dateShift(base string, years, days int) string {
	t, err := time.Parse("2006-01-02", base)
	if err != nil {
		return base
	}
	if years != 0 {
		t = t.AddDate(-years, 0, 0)
	} else {
		t = t.AddDate(0, 0, -days)
	}
	return t.Format("2006-01-02")
}

// dateRange 起止日期解析，失败返回空。
func dateRange(start, end string) (string, string) {
	if _, err := time.Parse("2006-01-02", start); err != nil {
		return "", ""
	}
	if _, err := time.Parse("2006-01-02", end); err != nil {
		return "", ""
	}
	if start > end {
		return end, start
	}
	return start, end
}

// dedupeUpsert 去重后批量写入。
func (e *Engine) dedupeUpsert(table string, cols, keys []string, rows []db.Row) error {
	if len(rows) == 0 {
		return nil
	}
	rows = db.Dedupe(rows, keys)
	return e.DB.BulkUpsert(table, cols, keys, rows, 6000)
}

// existingDatesWithCount 查询满足最小条数的已存在日期集合。
func (e *Engine) existingDatesWithCount(table, dateCol, start, end string, min int64) map[string]bool {
	out := map[string]bool{}
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN ? AND ? GROUP BY %s HAVING COUNT(*)>=?",
		dateCol, table, dateCol, dateCol)
	rows, err := e.DB.QueryRows(q, start, end, min)
	if err != nil {
		return out
	}
	for _, r := range rows {
		if s := strVal(r[dateCol]); s != "" {
			out[s[:10]] = true
		}
	}
	return out
}

// existingDates 查询已存在日期集合（无条数阈值）。
func (e *Engine) existingDates(table, dateCol, start, end string) map[string]bool {
	out := map[string]bool{}
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN ? AND ? GROUP BY %s", dateCol, table, dateCol, dateCol)
	rows, err := e.DB.QueryRows(q, start, end)
	if err != nil {
		return out
	}
	for _, r := range rows {
		if s := strVal(r[dateCol]); s != "" {
			out[s[:10]] = true
		}
	}
	return out
}

// expectedCount 按最近完整日数据量乘系数估算单日最小条数。
func (e *Engine) expectedCount(table, dateCol string, factor float64) int64 {
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s=(SELECT MAX(%s) FROM %s WHERE %s<date('now','-1 day'))",
		table, dateCol, dateCol, table, dateCol)
	if n, err := e.DB.QueryInt(q); err == nil && n > 0 {
		if v := int64(float64(n) * factor); v > 1000 {
			return v
		}
		return 1000
	}
	if n, err := e.DB.QueryInt("SELECT COUNT(*) FROM stock_basic WHERE list_status='L'"); err == nil && n > 0 {
		if v := int64(float64(n) * 0.9); v > 1000 {
			return v
		}
		return 1000
	}
	return 1000
}

// stockCodes 查询全部股票代码。
func (e *Engine) stockCodes() ([]string, error) {
	return e.DB.QueryStrings("SELECT ts_code FROM stock_basic")
}

// logWarn 打警告日志（汇总级）。
func logWarn(format string, args ...any) { log.Printf("[WARN] "+format, args...) }
