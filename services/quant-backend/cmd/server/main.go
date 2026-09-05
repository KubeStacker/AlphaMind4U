// 量化后端入口：初始化库、启动任务消费者与调度器、对外 HTTP。
package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"quantbackend/internal/api"
	"quantbackend/internal/calendar"
	"quantbackend/internal/config"
	"quantbackend/internal/db"
	"quantbackend/internal/engine"
	"quantbackend/internal/provider"
	"quantbackend/internal/sched"
)

// nowShanghai 上海当前时间文本。
func nowShanghai() string {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		loc = time.FixedZone("CST", 8*3600)
	}
	return time.Now().In(loc).Format("2006-01-02 15:04:05")
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime)
	cfg := config.Load()
	log.Printf("量化后端启动: addr=%s db=%s", cfg.ListenAddr, cfg.DBPath)

	database, err := db.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}
	defer database.Close()
	if err := database.InitSchema(); err != nil {
		log.Fatalf("初始化数据表失败: %v", err)
	}
	if applied, err := database.Migrate(); err != nil {
		log.Fatalf("执行迁移失败: %v", err)
	} else if len(applied) > 0 {
		log.Printf("已应用迁移: %v", applied)
	}
	// 崩溃恢复：上次未跑完的 RUNNING 任务回 PENDING（upsert 幂等，可重跑）。
	if n, err := database.Exec(
		`UPDATE etl_tasks SET status='PENDING', heartbeat_at=? WHERE status='RUNNING'`, nowShanghai()); err != nil {
		log.Printf("任务恢复失败: %v", err)
	} else if n > 0 {
		log.Printf("恢复 %d 个中断任务为 PENDING", n)
	}

	prov := provider.New(cfg.TushareTokenType, cfg.ShortToken, cfg.LongToken, cfg.TushareAPIURL)
	cal := calendar.New(database)
	eng := engine.New(database, prov, cal)
	srv := api.NewWithAdmin(database, eng, cfg.StaticToken, cfg.AdminUser, cfg.AdminPassword)

	stop := make(chan struct{})
	defer close(stop)
	if cfg.EnableETLWorker {
		go srv.RunWorker(stop)
	} else {
		log.Println("ETL 任务消费者已按配置禁用")
	}
	var sc *sched.Scheduler
	if cfg.EnableScheduler {
		sc = sched.New(eng, srv.Enqueue)
		if err := sc.Start(); err != nil {
			log.Printf("调度器启动失败: %v", err)
		}
		defer sc.Stop()
	} else {
		log.Println("定时任务调度器已按配置禁用")
	}

	httpSrv := &http.Server{Addr: cfg.ListenAddr, Handler: srv.Handler()}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 服务异常退出: %v", err)
		}
	}()
	log.Println("量化后端就绪")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("正在关闭资源...")
	_ = httpSrv.Close()
}
