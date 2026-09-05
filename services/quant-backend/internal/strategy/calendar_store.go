// 回放日历：缺失不回退工作日，评估日独立于所选回测结束日期。
package strategy

import (
	"fmt"
	"sort"
	"time"
)

// LoadReplayWindow 加载预热窗口，兼容仅需要评估日的调用方。
func LoadReplayWindow(q Queryer, from, to string, n int) ([]string, map[string]bool, error) {
	dates, entries, _, err := LoadReplaySchedule(q, from, to, n)
	return dates, entries, err
}

// LoadReplaySchedule 冻结预热日历、评估日与下一交易日月份，账户起点仍由from指定。
func LoadReplaySchedule(q Queryer, from, to string, n int) ([]string, map[string]bool, map[string]string, error) {
	start, err := time.Parse("2006-01-02", from)
	if err != nil || n <= 0 {
		return nil, nil, nil, errInvalid("预热参数非法")
	}
	dates, entries, months, err := loadReplayCalendar(q, start.AddDate(0, 0, -366).Format("2006-01-02"), to)
	if err != nil {
		return nil, nil, nil, err
	}
	i := sort.SearchStrings(dates, from)
	if i == len(dates) || i < n-1 {
		return nil, nil, nil, errInvalid("回放区间无开市日或起点前预热日历不足")
	}
	return dates[i-n+1:], entries, months, nil
}

// LoadReplayCalendar 读取SSE完整日历及评估日，区间后首个开市日仅作边界。
func LoadReplayCalendar(q Queryer, from, to string) ([]string, map[string]bool, error) {
	dates, entries, _, err := loadReplayCalendar(q, from, to)
	return dates, entries, err
}

// loadReplayCalendar 同时冻结后继月份，不读取未来行情。
func loadReplayCalendar(q Queryer, from, to string) ([]string, map[string]bool, map[string]string, error) {
	start, err1 := time.Parse("2006-01-02", from)
	end, err2 := time.Parse("2006-01-02", to)
	if err1 != nil || err2 != nil || start.After(end) {
		return nil, nil, nil, errInvalid("回放日历区间非法")
	}
	rows, err := q.QueryRows("SELECT cal_date, is_open FROM trade_calendar"+
		" WHERE exchange='SSE' AND cal_date BETWEEN ? AND ? ORDER BY cal_date ASC LIMIT 2001",
		from, end.AddDate(0, 0, 31).Format("2006-01-02"))
	if err != nil {
		return nil, nil, nil, err
	}
	if len(rows) > 2000 {
		return nil, nil, nil, errInvalid("回放日历超出2000条上限")
	}
	dates := []string{}
	entries := map[string]bool{}
	months := map[string]string{}
	expected := start
	var previous time.Time
	for _, row := range rows {
		date := strOf(row["cal_date"])
		if date != expected.Format("2006-01-02") {
			return nil, nil, nil, fmt.Errorf("%s: 回放日历缺失或日期非法", expected.Format("2006-01-02"))
		}
		flag := strOf(row["is_open"])
		if flag != "0" && flag != "1" {
			return nil, nil, nil, fmt.Errorf("%s: 回放日历开市标记非法", date)
		}
		if flag == "1" {
			if !previous.IsZero() {
				months[previous.Format("2006-01-02")] = expected.Format("2006-01")
				py, pw := previous.ISOWeek()
				y, w := expected.ISOWeek()
				entries[previous.Format("2006-01-02")] = py != y || pw != w ||
					previous.Format("2006-01") != expected.Format("2006-01")
			}
			if expected.After(end) {
				if len(dates) == 0 {
					return nil, nil, nil, errInvalid("回放区间无开市日")
				}
				return dates, entries, months, nil
			}
			dates = append(dates, date)
			previous = expected
		}
		expected = expected.AddDate(0, 0, 1)
	}
	return nil, nil, nil, fmt.Errorf("%s起: 日历不完整或缺少结束日后的开市日", expected.Format("2006-01-02"))
}
