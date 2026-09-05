// ETL 采集任务队列接口：入队、消费、状态查询。
package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"quantbackend/internal/engine"
	"quantbackend/internal/provider"
)

// allowedTasks 允许的任务名，对齐旧 ALLOWED_TASKS。
var allowedTasks = map[string]bool{
	"basic": true, "calendar": true, "price": true, "daily": true,
	"moneyflow": true, "daily_basic": true, "concepts": true, "index": true,
	"index_members": true, "express": true, "income": true, "financials": true,
	"balancesheet": true, "balance_sheet": true, "cashflow": true,
	"fina_indicator": true, "dividend": true, "forecast": true,
	"suspend": true, "namechange": true, "index_weight": true,
	"moneyflow_hsgt": true, "hsgt": true, "margin": true, "daily_update": true,
}

// syncReq 入队请求体。
type syncReq struct {
	Task      string `json:"task"`
	Years     int    `json:"years"`
	Days      int    `json:"days"`
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
	TsCode    string `json:"ts_code"`
	Force     bool   `json:"force"`
}

// newTaskID 随机任务 ID。
func newTaskID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// fullTasks 全量型任务，磁盘 ≥90% 时拒绝（只允许增量）。
var fullTasks = map[string]bool{
	"basic": true, "calendar": true, "concepts": true, "index_members": true,
}

// diskUsagePct 返回 data 目录所在磁盘使用率百分比。
func diskUsagePct() int {
	var fs syscallStatfs
	if err := statfsRoot(&fs); err != nil {
		return 0
	}
	if fs.blocks == 0 {
		return 0
	}
	return int((fs.blocks - fs.bfree) * 100 / fs.blocks)
}

// handleTriggerSync 任务入队，202 返回；重复提交返回已有任务。
func (s *Server) handleTriggerSync(w http.ResponseWriter, r *http.Request) {
	var req syncReq
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "请求体非法"})
		return
	}
	req.Task = strings.ToLower(strings.TrimSpace(req.Task))
	if !allowedTasks[req.Task] {
		names := make([]string, 0, len(allowedTasks))
		for n := range allowedTasks {
			names = append(names, n)
		}
		writeJSON(w, http.StatusBadRequest,
			map[string]string{"detail": "不支持的采集任务，可选值: " + strings.Join(names, ", ")})
		return
	}
	if req.Days <= 0 {
		req.Days = 3
	}
	if diskUsagePct() >= diskRejectPct() && (req.Force || req.Years > 0 || fullTasks[req.Task]) {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"detail": "磁盘使用率超限，已拒绝全量任务，只允许增量同步"})
		return
	}
	taskID, dup, err := s.Enqueue(engine.SyncParams{
		Task: req.Task, Years: req.Years, Days: req.Days,
		StartDate: req.StartDate, EndDate: req.EndDate, TsCode: req.TsCode, Force: req.Force,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "入队失败"})
		return
	}
	msg := "采集任务已加入队列"
	if dup {
		msg = "相同任务已在队列，返回已有任务"
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"message": msg, "task_id": taskID})
}

// Enqueue 任务入队；同参数 PENDING/RUNNING 已存在则返回其 ID（dup=true）。
//
// 终态（COMPLETED/FAILED/SKIPPED）后同参可新入队，支持失败续跑：
func (s *Server) Enqueue(p engine.SyncParams) (taskID string, dup bool, err error) {
	canonical, _ := json.Marshal(map[string]any{
		"task": p.Task, "years": p.Years, "days": p.Days,
		"start_date": p.StartDate, "end_date": p.EndDate,
		"ts_code": p.TsCode, "force": p.Force,
	})
	rows, err := s.DB.QueryRows(`SELECT task_id, params_json FROM etl_tasks
		WHERE status IN ('PENDING','RUNNING') ORDER BY created_at LIMIT 100`)
	if err != nil {
		return "", false, err
	}
	for _, r := range rows {
		if strOf(r["params_json"]) == string(canonical) {
			return strOf(r["task_id"]), true, nil
		}
	}
	taskID = newTaskID()
	_, err = s.DB.Exec(`INSERT INTO etl_tasks (task_id, task_key, task_type, params_json, status, progress, created_at)
		VALUES (?, ?, 'SYNC', ?, 'PENDING', 0, ?)`, taskID, taskID, string(canonical), nowStr())
	if err != nil {
		return "", false, err
	}
	return taskID, false, nil
}

// handleTasksStatus 任务状态倒序查询。
func (s *Server) handleTasksStatus(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	if limit < 1 {
		limit = 1
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.DB.QueryRows(`SELECT task_id, task_type, status, error, progress,
		created_at, finished_at FROM etl_tasks ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "查询失败"})
		return
	}
	out := make([]map[string]any, 0, len(rows))
	for _, t := range rows {
		out = append(out, map[string]any{
			"task_id": t["task_id"], "task_type": t["task_type"], "status": t["status"],
			"error": t["error"], "progress": t["progress"],
			"created_at": t["created_at"], "finished_at": t["finished_at"],
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": out})
}

// RunWorker 任务消费者：轮询 PENDING 并串行执行。
func (s *Server) RunWorker(stop <-chan struct{}) {
	if _, err := s.DB.Exec("UPDATE strategy_runs SET status='PENDING',updated_at=? WHERE status='RUNNING'", nowStr()); err != nil {
		log.Printf("研究任务恢复失败: %v", err)
	}
	log.Println("原始数据采集任务消费者已启动")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			pending, err := s.DB.QueryInt("SELECT COUNT(*) FROM etl_tasks WHERE status='PENDING'")
			if err != nil {
				log.Printf("任务队列读取失败: %v", err)
				continue
			}
			if pending > 0 {
				s.consumeOne()
			} else {
				s.consumeBacktest(stop)
			}
		}
	}
}

// consumeOne 取一件 PENDING 执行。
func (s *Server) consumeOne() {
	rows, err := s.DB.QueryRows(`SELECT task_id, params_json FROM etl_tasks
		WHERE status='PENDING' ORDER BY created_at LIMIT 1`)
	if err != nil || len(rows) == 0 {
		return
	}
	taskID := strOf(rows[0]["task_id"])
	var raw map[string]any
	if err := json.Unmarshal([]byte(strOf(rows[0]["params_json"])), &raw); err != nil {
		s.updateTask(taskID, "FAILED", 0, "参数解析失败")
		return
	}
	s.updateTask(taskID, "RUNNING", 1, "")
	if err := s.Engine.RunTask(engine.SyncParams{
		Task:      strOf(raw["task"]),
		Years:     intOf(raw["years"]),
		Days:      intOf(raw["days"]),
		StartDate: strOf(raw["start_date"]),
		EndDate:   strOf(raw["end_date"]),
		TsCode:    strOf(raw["ts_code"]),
		Force:     boolOf(raw["force"]),
	}); err != nil {
		var perm *provider.PermissionError
		if errors.As(err, &perm) {
			log.Printf("采集任务 %s 无权限，记 SKIPPED: %v", taskID, err)
			s.updateTask(taskID, "SKIPPED", 100, trunc(err.Error(), 500))
			return
		}
		log.Printf("采集任务 %s 失败: %v", taskID, err)
		s.updateTask(taskID, "FAILED", 0, trunc(err.Error(), 500))
		return
	}
	s.updateTask(taskID, "COMPLETED", 100, "")
}

// updateTask 更新任务状态与心跳。
func (s *Server) updateTask(taskID, status string, progress float64, errMsg string) {
	now := nowStr()
	if status == "RUNNING" {
		_, _ = s.DB.Exec(`UPDATE etl_tasks SET status=?, progress=?, error=?,
			started_at=COALESCE(started_at, ?), heartbeat_at=? WHERE task_id=?`,
			status, progress, nilStr(errMsg), now, now, taskID)
		return
	}
	_, _ = s.DB.Exec(`UPDATE etl_tasks SET status=?, progress=?, error=?,
		heartbeat_at=?, finished_at=? WHERE task_id=?`,
		status, progress, nilStr(errMsg), now, now, taskID)
}

// nilStr 空串转 nil 存 NULL。
func nilStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// trunc 截断字符串。
func trunc(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}

// strOf 转字符串，nil 返回空串。
func strOf(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// intOf 转整数。
func intOf(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

// boolOf 转布尔。
func boolOf(v any) bool {
	b, _ := v.(bool)
	return b
}
