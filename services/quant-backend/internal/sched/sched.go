// Package sched 定时采集调度器（上海时区），对齐旧 scheduler.py。
package sched

import (
	"log"
	"time"

	"github.com/robfig/cron/v3"
	"quantbackend/internal/engine"
)

// Scheduler 定时器持有者，触发只入队不直跑，保证任务全局串行。
type Scheduler struct {
	cron *cron.Cron
	// enqueue 统一入队函数，返回任务 ID。
	enqueue func(engine.SyncParams) (string, bool, error)
}

// New 构造调度器。
func New(e *engine.Engine, enqueue func(engine.SyncParams) (string, bool, error)) *Scheduler {
	_ = e
	return &Scheduler{cron: cron.New(cron.WithLocation(shanghai())), enqueue: enqueue}
}

// shanghai 加载上海时区，失败回退东八区。
func shanghai() *time.Location {
	if loc, err := time.LoadLocation("Asia/Shanghai"); err == nil {
		return loc
	}
	return time.FixedZone("CST", 8*3600)
}

// Start 注册任务并启动，对齐旧时间表。
func (s *Scheduler) Start() error {
	enqueue := s.enqueue
	submit := func(label string, p engine.SyncParams) {
		id, dup, err := enqueue(p)
		if err != nil {
			log.Printf("定时任务入队失败 %s: %v", label, err)
			return
		}
		if dup {
			log.Printf("定时任务 %s 已在队列，复用 %s", label, shortID(id))
			return
		}
		log.Printf("定时任务已入队 %s: %s", label, shortID(id))
	}
	jobs := []struct {
		spec string
		name string
		fn   func()
	}{
		{"5 0 * * *", "同步股票基础信息", func() {
			submit("basic", mkTask("basic"))
		}},
		{"45 16 * * *", "每日收盘数据更新(主)", func() {
			submit("daily_update", mkTask("daily_update"))
		}},
		{"30 18 * * *", "每日收盘数据更新(兜底)", func() {
			submit("daily_update", mkTask("daily_update"))
		}},
		{"7 19 * * *", "每日融资融券数据同步", func() {
			submit("margin", mkTaskWith("margin", 0, 3))
		}},
		{"0 2 * * 0", "每周财务报表同步", func() {
			submit("income", mkTask("income"))
			submit("balancesheet", mkTask("balancesheet"))
			submit("cashflow", mkTask("cashflow"))
		}},
	}
	for _, j := range jobs {
		name, fn := j.name, j.fn
		if _, err := s.cron.AddFunc(j.spec, func() {
			log.Printf("定时任务触发: %s", name)
			fn()
		}); err != nil {
			return err
		}
	}
	s.cron.Start()
	log.Println("定时任务调度器已启动: [基础数据] 股票基础信息(00:05) | " +
		"[行情数据] 每日更新(16:45) | 每日更新兜底(18:30) | " +
		"[资金融资] 融资融券(19:07) | [财务数据] 财务报表(周日02:00)")
	return nil
}

// Stop 停止调度器。
func (s *Scheduler) Stop() { s.cron.Stop() }

// mkTask 构造默认任务参数。
func mkTask(name string) engine.SyncParams {
	return engine.SyncParams{Task: name, Days: 3}
}

// mkTaskWith 构造带回溯天数的任务参数。
func mkTaskWith(name string, years, days int) engine.SyncParams {
	return engine.SyncParams{Task: name, Years: years, Days: days}
}

// shortID 任务 ID 缩写。
func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
