// 研究任务HTTP：提交、只读查询和取消，不在请求中计算策略。
package api

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

const strategyModelVersion = "2026-09-09.7"

// enqueueBacktest 保存规范化请求，活动重复请求复用同一任务。
func (s *Server) enqueueBacktest(w http.ResponseWriter, req backtestReq) {
	s.strategyMu.Lock()
	defer s.strategyMu.Unlock()
	body, err := json.Marshal(req)
	if err != nil {
		writeJSON(w, 400, map[string]string{"detail": "账户请求无法保存"})
		return
	}
	key := fmt.Sprintf("%x", sha256.Sum256(append([]byte(strategyModelVersion), body...)))
	rows, err := s.DB.QueryRows("SELECT run_id,status FROM strategy_runs"+
		" WHERE request_key=? AND status IN ('PENDING','RUNNING') LIMIT 1", key)
	if err != nil {
		writeJSON(w, 500, map[string]string{"detail": "研究队列读取失败"})
		return
	}
	if len(rows) > 0 {
		writeJSON(w, 202, rows[0])
		return
	}
	n, err := s.DB.QueryInt("SELECT COUNT(*) FROM strategy_runs WHERE status IN ('PENDING','RUNNING')")
	if err != nil {
		writeJSON(w, 500, map[string]string{"detail": "研究队列读取失败"})
		return
	}
	if n >= 20 {
		writeJSON(w, 429, map[string]string{"detail": "活动研究任务已满"})
		return
	}
	id := newTaskID()
	_, err = s.DB.Exec("INSERT INTO strategy_runs"+
		" (run_id,request_key,request_json,status,model_version,created_at,updated_at) VALUES (?,?,?,'PENDING',?,?,?)",
		id, key, string(body), strategyModelVersion, nowStr(), nowStr())
	if err != nil {
		writeJSON(w, 500, map[string]string{"detail": "研究任务入队失败"})
		return
	}
	writeJSON(w, 202, map[string]string{"run_id": id, "status": "PENDING"})
}

// handleListBacktests 只读最近研究任务，不返回大型输入快照。
func (s *Server) handleListBacktests(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 20
	}
	limit = min(limit, 100)
	rows, err := s.DB.QueryRows("SELECT run_id,status,progress,error,input_hash,model_version,created_at,updated_at"+
		" FROM strategy_runs ORDER BY created_at DESC,run_id DESC LIMIT ?", limit)
	if err != nil {
		writeJSON(w, 500, map[string]string{"detail": "研究任务查询失败"})
		return
	}
	writeJSON(w, 200, map[string]any{"runs": rows})
}

// handleGetBacktest 只读保存的任务详情及完整结果。
func (s *Server) handleGetBacktest(w http.ResponseWriter, r *http.Request) {
	rows, err := s.DB.QueryRows("SELECT run_id,status,progress,error,input_hash,model_version,created_at,updated_at,"+
		" request_json,result_json FROM strategy_runs WHERE run_id=? LIMIT 1", r.PathValue("id"))
	if err != nil {
		writeJSON(w, 500, map[string]string{"detail": "研究任务读取失败"})
		return
	}
	if len(rows) == 0 {
		writeJSON(w, 404, map[string]string{"detail": "研究任务不存在"})
		return
	}
	row := rows[0]
	row["request"] = json.RawMessage(strOf(row["request_json"]))
	if strOf(row["status"]) == "COMPLETED" {
		row["result"] = json.RawMessage(strOf(row["result_json"]))
	} else if body := strOf(row["result_json"]); body != "" {
		var diagnostic map[string]json.RawMessage
		if err := json.Unmarshal([]byte(body), &diagnostic); err != nil {
			writeJSON(w, 500, map[string]string{"detail": "任务诊断读取失败"})
			return
		}
		if quality, ok := diagnostic["data_quality"]; ok {
			row["data_quality"] = quality
		}
	}
	delete(row, "request_json")
	delete(row, "result_json")
	writeJSON(w, 200, row)
}

// handleCancelBacktest 取消活动任务，保留请求和输入供审查。
func (s *Server) handleCancelBacktest(w http.ResponseWriter, r *http.Request) {
	n, err := s.DB.Exec("UPDATE strategy_runs SET status='CANCELLED',updated_at=?"+
		" WHERE run_id=? AND status IN ('PENDING','RUNNING')", nowStr(), r.PathValue("id"))
	if err != nil {
		writeJSON(w, 500, map[string]string{"detail": "取消失败"})
		return
	}
	if n == 0 {
		writeJSON(w, 409, map[string]string{"detail": "任务不是活动状态或不存在"})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "CANCELLED"})
}
