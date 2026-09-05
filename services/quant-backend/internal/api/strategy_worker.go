// 研究消费者：冻结输入、同版本恢复、取消检查与原子结果发布。
package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"quantbackend/internal/strategy"
)

// replaySnapshot 保存纯计算所需全部输入，不包含数据库连接或函数回调。
type replaySnapshot struct {
	S0  *strategy.S0Input  `json:"s0,omitempty"`
	S12 *strategy.S12Input `json:"s12,omitempty"`
}

// consumeBacktest 单消费者在无待处理ETL时运行一件研究任务。
func (s *Server) consumeBacktest(stop <-chan struct{}) {
	rows, err := s.DB.QueryRows("SELECT run_id,request_json,input_json,input_hash,model_version FROM strategy_runs" +
		" WHERE status='PENDING' ORDER BY created_at,run_id LIMIT 1")
	if err != nil || len(rows) == 0 {
		return
	}
	row := rows[0]
	id := strOf(row["run_id"])
	n, err := s.DB.Exec("UPDATE strategy_runs SET status='RUNNING',progress=1,updated_at=?"+
		" WHERE run_id=? AND status='PENDING'", nowStr(), id)
	if err != nil || n != 1 {
		return
	}
	finishError := func(status string, err error) {
		log.Printf("研究任务 %s %s: %v", id, status, err)
		if _, saveErr := s.DB.Exec("UPDATE strategy_runs SET status=?,error=?,updated_at=?"+
			" WHERE run_id=? AND status='RUNNING'", status, trunc(err.Error(), 1000), nowStr(), id); saveErr != nil {
			log.Printf("研究任务状态保存失败 %s: %v", id, saveErr)
		}
	}
	defer func() {
		if value := recover(); value != nil {
			finishError("FAILED", fmt.Errorf("计算异常: %v", value))
		}
	}()
	if strOf(row["model_version"]) != strategyModelVersion {
		finishError("BLOCKED", fmt.Errorf("模型版本不同，不能用新代码重放旧任务"))
		return
	}
	var req backtestReq
	if err := json.Unmarshal([]byte(strOf(row["request_json"])), &req); err != nil {
		finishError("FAILED", err)
		return
	}
	if !validVersions[req.Version] {
		finishError("BLOCKED", fmt.Errorf("策略 %s 已停用或不受支持，历史结果仍可读取", req.Version))
		return
	}
	var snapshot replaySnapshot
	input := strOf(row["input_json"])
	hash := strOf(row["input_hash"])
	if input == "" {
		effectiveParams := req.paramsConfig()
		if req.Version == "S0" {
			in, err := s.Engine.PrepareS0(req.EffectMonths, req.From, req.To, req.accountConfig(), req.DataPolicy)
			if err != nil {
				finishError("BLOCKED", err)
				return
			}
			in.Params = effectiveParams
			snapshot.S0 = &in
		} else {
			in, err := s.Engine.PrepareS12(strategy.S12Version(req.Version), req.EffectMonths,
				req.From, req.To, nil, req.accountConfig(), req.DataPolicy)
			if err != nil {
				finishError("BLOCKED", err)
				return
			}
			in.Params = effectiveParams
			snapshot.S12 = &in
		}
		body, err := json.Marshal(snapshot)
		if err != nil {
			finishError("FAILED", err)
			return
		}
		input, hash = string(body), fmt.Sprintf("%x", sha256.Sum256(body))
		var quality strategy.DataQuality
		if snapshot.S0 != nil {
			quality = snapshot.S0.DataQuality
		} else {
			quality = snapshot.S12.DataQuality
		}
		diagnostics, err := json.Marshal(map[string]any{"data_quality": quality})
		if err != nil {
			finishError("FAILED", err)
			return
		}
		n, err := s.DB.Exec("UPDATE strategy_runs SET input_json=?,input_hash=?,result_json=?,progress=5,updated_at=?"+
			" WHERE run_id=? AND status='RUNNING'", input, hash, string(diagnostics), nowStr(), id)
		if err != nil {
			finishError("FAILED", err)
			return
		}
		if n != 1 {
			return
		}
	} else {
		if fmt.Sprintf("%x", sha256.Sum256([]byte(input))) != hash {
			finishError("FAILED", fmt.Errorf("冻结输入hash不匹配"))
			return
		}
		if err := json.Unmarshal([]byte(input), &snapshot); err != nil {
			finishError("FAILED", err)
			return
		}
	}
	checkpoint := func(done, total int) error {
		select {
		case <-stop:
			return context.Canceled
		default:
		}
		if done%10 != 0 {
			return nil
		}
		status, ok, err := s.DB.QueryValue("SELECT status FROM strategy_runs WHERE run_id=? LIMIT 1", id)
		if err != nil {
			return err
		}
		if !ok || strOf(status) != "RUNNING" {
			return context.Canceled
		}
		_, err = s.DB.Exec("UPDATE strategy_runs SET progress=?,updated_at=? WHERE run_id=? AND status='RUNNING'",
			5+94*float64(done)/float64(max(1, total)), nowStr(), id)
		return err
	}
	var result map[string]any
	if req.Version == "S0" && snapshot.S0 != nil && snapshot.S12 == nil {
		snapshot.S0.Checkpoint = checkpoint
		res := strategy.RunS0Backtest(*snapshot.S0)
		err = res.Err
		if err == nil {
			result = s0Response(req, res)
		}
	} else if req.Version != "S0" && snapshot.S12 != nil && snapshot.S0 == nil {
		snapshot.S12.Checkpoint = checkpoint
		res := strategy.RunS12Backtest(*snapshot.S12)
		err = res.Err
		if err == nil {
			result = s12Response(req, res)
		}
	} else {
		err = fmt.Errorf("冻结输入类型与策略不一致")
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	if err != nil {
		finishError("BLOCKED", err)
		return
	}
	if snapshot.S0 != nil {
		result["pools"] = poolsOf(snapshot.S0.Pools)
	} else {
		result["pools"] = poolsOf(snapshot.S12.Pools)
	}
	result["evidence_level"] = "engineering_replay"
	if snapshot.S0 != nil {
		result["data_quality"] = snapshot.S0.DataQuality
	} else {
		result["data_quality"] = snapshot.S12.DataQuality
	}
	if req.DataPolicy == "available_candidates" {
		result["evidence_level"] = "limited_research"
	}
	result["limitations"] = []string{
		"全市场覆盖、历史可用时间及交易资格尚未完整验收",
		"公司行动权益账本、历史费用与成交规则尚未完整验收",
		"市场输入尚未装配，年度有效样本及样本外验证未完成",
	}
	result["input_hash"], result["model_version"] = hash, strategyModelVersion
	body, err := json.Marshal(result)
	if err != nil {
		finishError("FAILED", err)
		return
	}
	if _, err := s.DB.Exec("UPDATE strategy_runs SET status='COMPLETED',progress=100,result_json=?,updated_at=?"+
		" WHERE run_id=? AND status='RUNNING'", string(body), nowStr(), id); err != nil {
		finishError("FAILED", err)
	}
}
