// Package engine ETL 同步引擎：装配 DB/Provider/Calendar，任务实现见 task_*.go。
package engine

import (
	"quantbackend/internal/calendar"
	"quantbackend/internal/db"
	"quantbackend/internal/provider"
)

// SyncParams 单次同步任务参数，对齐旧 SyncTaskParams。
type SyncParams struct {
	// Task 任务名小写，如 price/daily_update。
	Task string
	// Years 回溯年数。
	Years int
	// Days 回溯天数。
	Days int
	// StartDate 起始日期 YYYY-MM-DD。
	StartDate string
	// EndDate 结束日期 YYYY-MM-DD。
	EndDate string
	// TsCode 单股票/指数代码。
	TsCode string
	// Force 强制重同步已存在数据。
	Force bool
}

// Engine 同步引擎，持有共享依赖。
type Engine struct {
	DB  *db.DB
	P   *provider.Provider
	Cal *calendar.Calendar
}

// New 构造引擎。
func New(d *db.DB, p *provider.Provider, cal *calendar.Calendar) *Engine {
	return &Engine{DB: d, P: p, Cal: cal}
}
