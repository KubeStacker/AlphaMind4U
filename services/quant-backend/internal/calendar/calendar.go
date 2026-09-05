// Package calendar A股交易日历，优先读库内 trade_calendar。
package calendar

import (
	"time"

	"quantbackend/internal/db"
)

// Calendar 交易日历查询器。
type Calendar struct {
	db  *db.DB
	loc *time.Location
}

// New 构造日历，时区固定上海。
func New(d *db.DB) *Calendar {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		loc = time.FixedZone("CST", 8*3600)
	}
	return &Calendar{db: d, loc: loc}
}

// Now 上海当前时间。
func (c *Calendar) Now() time.Time { return time.Now().In(c.loc) }

// Today 上海今天 YYYY-MM-DD。
func (c *Calendar) Today() string { return c.Now().Format("2006-01-02") }

// IsTradingDay 是否交易日，库无记录回退到工作日判断。
func (c *Calendar) IsTradingDay(day string) bool {
	v, ok, err := c.db.QueryValue(
		"SELECT is_open FROM trade_calendar WHERE exchange='SSE' AND cal_date=?", day)
	if err == nil && ok && v != nil {
		switch n := v.(type) {
		case int64:
			return n != 0
		case float64:
			return n != 0
		}
	}
	t, err := time.ParseInLocation("2006-01-02", day, c.loc)
	if err != nil {
		return false
	}
	return t.Weekday() != time.Saturday && t.Weekday() != time.Sunday
}

// LastTradingDay 指定日期之前最后一个交易日（不含当日）。
func (c *Calendar) LastTradingDay(ref string) string {
	v, ok, err := c.db.QueryValue(
		`SELECT cal_date FROM trade_calendar WHERE exchange='SSE'
		 AND is_open=1 AND cal_date<? ORDER BY cal_date DESC LIMIT 1`, ref)
	if err == nil && ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	t, err := time.ParseInLocation("2006-01-02", ref, c.loc)
	if err != nil {
		return ref
	}
	for i := 1; i <= 10; i++ {
		d := t.AddDate(0, 0, -i).Format("2006-01-02")
		if c.IsTradingDay(d) {
			return d
		}
	}
	return ref
}

// LatestSyncDate 可同步的最晚日期：交易日 15:30 后含今日，否则上个交易日。
func (c *Calendar) LatestSyncDate() string {
	now := c.Now()
	today := now.Format("2006-01-02")
	if c.IsTradingDay(today) && now.Hour()*60+now.Minute() >= 15*60+30 {
		return today
	}
	return c.LastTradingDay(today)
}

// TradingDays 区间内交易日列表（起止 YYYY-MM-DD）。
func (c *Calendar) TradingDays(start, end string) []string {
	rows, err := c.db.QueryRows(
		`SELECT cal_date FROM trade_calendar WHERE exchange='SSE' AND is_open=1
		 AND cal_date BETWEEN ? AND ? ORDER BY cal_date`, start, end)
	if err == nil && len(rows) > 0 {
		out := make([]string, 0, len(rows))
		for _, r := range rows {
			if s, ok := r["cal_date"].(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	var out []string
	s, err1 := time.ParseInLocation("2006-01-02", start, c.loc)
	e, err2 := time.ParseInLocation("2006-01-02", end, c.loc)
	if err1 != nil || err2 != nil {
		return out
	}
	for d := s; !d.After(e); d = d.AddDate(0, 0, 1) {
		if wd := d.Weekday(); wd != time.Saturday && wd != time.Sunday {
			out = append(out, d.Format("2006-01-02"))
		}
	}
	return out
}
