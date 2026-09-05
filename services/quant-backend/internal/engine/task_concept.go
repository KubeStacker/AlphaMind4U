// Package engine 概念分类任务：staging 写入后去重发布。
package engine

import (
	"fmt"
	"log"

	"quantbackend/internal/db"
)

// SyncConceptClassification 同步概念分类与成分股。
func (e *Engine) SyncConceptClassification() error {
	log.Printf("开始同步概念数据...")
	if err := e.prepareConceptStaging(); err != nil {
		return err
	}
	defer e.cleanupConceptStaging()
	synced, err := e.syncThsConcepts()
	if err != nil {
		return err
	}
	if !synced {
		return fmt.Errorf("THS概念为空，保留现有数据")
	}
	if err := e.publishConceptStaging(); err != nil {
		return err
	}
	log.Printf("概念数据已原子化发布")
	return nil
}

// prepareConceptStaging 建空 staging 表。
func (e *Engine) prepareConceptStaging() error {
	e.cleanupConceptStaging()
	stmts := []string{
		"CREATE TABLE stock_concepts__staging (code TEXT, name TEXT, src TEXT)",
		"CREATE TABLE stock_concept_details__staging (id TEXT, concept_name TEXT, ts_code TEXT, name TEXT)",
	}
	for _, s := range stmts {
		if _, err := e.DB.Exec(s); err != nil {
			return err
		}
	}
	return nil
}

// cleanupConceptStaging 清理 staging 与中间表。
func (e *Engine) cleanupConceptStaging() {
	for _, t := range []string{"stock_concepts__staging", "stock_concept_details__staging",
		"stock_concepts__publish", "stock_concept_details__publish",
		"stock_concepts__backup", "stock_concept_details__backup"} {
		_, _ = e.DB.Exec("DROP TABLE IF EXISTS " + t)
	}
}

// normalizeConceptCatalog 清洗概念目录并按 code 去重。
func normalizeConceptCatalog(rows []db.Row) []db.Row {
	seen := map[string]bool{}
	out := make([]db.Row, 0, len(rows))
	for _, r := range rows {
		code := strVal(r["code"])
		if code == "" {
			continue
		}
		if seen[code] {
			continue
		}
		seen[code] = true
		out = append(out, db.Row{"code": code, "name": strVal(r["name"]), "src": strVal(r["src"])})
	}
	return out
}

// normalizeConceptDetail 清洗概念明细并按主键去重。
func normalizeConceptDetail(rows []db.Row) []db.Row {
	seen := map[string]bool{}
	out := make([]db.Row, 0, len(rows))
	for _, r := range rows {
		id := strVal(r["id"])
		code := strVal(r["ts_code"])
		if id == "" || code == "" {
			continue
		}
		k := id + "\x00" + code
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, db.Row{"id": id, "concept_name": strVal(r["concept_name"]),
			"ts_code": code, "name": strVal(r["name"])})
	}
	return out
}

// syncThsConcepts 同花顺概念写入 staging。
func (e *Engine) syncThsConcepts() (bool, error) {
	ths, err := e.P.ThsIndex("A", "N")
	if err != nil {
		return false, err
	}
	if len(ths) == 0 || len(ths) >= 5000 {
		return false, fmt.Errorf("THS概念目录为空或达到来源上限")
	}
	catalog := make([]db.Row, 0, len(ths))
	for _, row := range ths {
		catalog = append(catalog, db.Row{"code": strVal(row["ts_code"]),
			"name": strVal(row["name"]), "src": "ths"})
	}
	catalog = normalizeConceptCatalog(catalog)
	if len(catalog) != len(ths) {
		return false, fmt.Errorf("THS概念目录缺代码或存在重复")
	}
	if err := e.dedupeUpsert("stock_concepts__staging",
		[]string{"code", "name", "src"}, []string{"code"}, catalog); err != nil {
		return false, err
	}
	total := 0
	for i, c := range ths {
		code, name := strVal(c["ts_code"]), strVal(c["name"])
		detail, err := e.P.ThsMember(code, "")
		if err != nil {
			return false, fmt.Errorf("THS成分 %s: %w", code, err)
		}
		if len(detail) == 0 {
			if strVal(c["count"]) != "0" {
				logWarn("THS概念 %s(%s) 上游成分返回空，跳过成分明细入库", code, name)
			}
			sleepNormal()
			continue
		}
		rows := make([]db.Row, 0, len(detail))
		for _, d := range detail {
			if strVal(d["ts_code"]) != code || strVal(d["con_code"]) == "" {
				return false, fmt.Errorf("THS成分 %s 关联代码非法", code)
			}
			rows = append(rows, db.Row{"id": code, "concept_name": name,
				"ts_code": strVal(d["con_code"]), "name": strVal(d["con_name"])})
		}
		if err := e.dedupeUpsert("stock_concept_details__staging",
			[]string{"id", "concept_name", "ts_code", "name"}, []string{"id", "ts_code"}, rows); err != nil {
			return false, err
		}
		total += len(rows)
		if (i+1)%20 == 0 {
			log.Printf("THS概念进度: %d/%d，来源成分%d条", i+1, len(ths), total)
		}
		sleepNormal()
	}
	log.Printf("THS概念明细同步完成: %d个概念，来源成分%d条", len(ths), total)
	return total > 0, nil
}

// syncTsConcepts 通用概念接口写入 staging。
func (e *Engine) syncTsConcepts() (bool, error) {
	list, err := e.P.Concept()
	if err != nil {
		return false, err
	}
	if len(list) == 0 {
		log.Printf("概念目录为空")
		return false, nil
	}
	catalog := normalizeConceptCatalog(list)
	if len(catalog) == 0 {
		return false, nil
	}
	_, _ = e.DB.Exec("DELETE FROM stock_concepts__staging")
	_ = e.dedupeUpsert("stock_concepts__staging",
		[]string{"code", "name", "src"}, []string{"code"}, catalog)
	_, _ = e.DB.Exec("DELETE FROM stock_concept_details__staging")
	var buf []db.Row
	total, count := 0, 0
	for _, c := range list {
		code := strVal(c["code"])
		detail, err := e.P.ConceptDetail(code)
		if err != nil || len(detail) == 0 {
			continue
		}
		buf = append(buf, normalizeConceptDetail(detail)...)
		count++
		if len(buf) > 0 && count%25 == 0 {
			_ = e.dedupeUpsert("stock_concept_details__staging",
				[]string{"id", "concept_name", "ts_code", "name"},
				[]string{"id", "ts_code"}, buf)
			total += len(buf)
			buf = nil
		}
		if count%50 == 0 {
			log.Printf("概念进度: %d/%d, 已插入 %d 条", count, len(list), total)
		}
		sleepNormal()
	}
	if len(buf) > 0 {
		_ = e.dedupeUpsert("stock_concept_details__staging",
			[]string{"id", "concept_name", "ts_code", "name"}, []string{"id", "ts_code"}, buf)
		total += len(buf)
	}
	log.Printf("概念明细同步完成: %d 个概念, %d 条记录", count, total)
	return total > 0, nil
}

// publishConceptStaging 去重发布 staging 到正式表。
//
// 全程单事务：建带主键的新表 → 去重导入 → 原子换表 → 重建索引，
// 任一步失败整体回滚，正式表不出现半发布状态。
func (e *Engine) publishConceptStaging() error {
	cat, err := e.DB.QueryInt("SELECT COUNT(*) FROM stock_concepts__staging")
	if err != nil {
		return err
	}
	det, err := e.DB.QueryInt("SELECT COUNT(*) FROM stock_concept_details__staging")
	if err != nil {
		return err
	}
	if cat <= 0 || det <= 0 {
		return fmt.Errorf("staging 概念数据不完整，拒绝覆盖现有库")
	}
	dup, _ := e.DB.QueryInt(`SELECT COUNT(*) FROM (SELECT id, ts_code FROM ` +
		`stock_concept_details__staging GROUP BY 1, 2 HAVING COUNT(*)>1)`)
	if dup > 0 {
		logWarn("staging 概念明细存在 %d 组重复主键，发布时窗口函数去重", dup)
	}
	stmts := []string{
		"DROP TABLE IF EXISTS stock_concepts_new",
		"DROP TABLE IF EXISTS stock_concept_details_new",
		"CREATE TABLE stock_concepts_new (code TEXT PRIMARY KEY, name TEXT, src TEXT)",
		"CREATE TABLE stock_concept_details_new " +
			"(id TEXT, concept_name TEXT, ts_code TEXT, name TEXT, PRIMARY KEY (id, ts_code))",
		`INSERT INTO stock_concepts_new (code, name, src) SELECT code, name, src FROM (` +
			`SELECT code, name, src, ROW_NUMBER() OVER (PARTITION BY code ORDER BY name, src) AS rn ` +
			`FROM stock_concepts__staging) WHERE rn=1`,
		`INSERT INTO stock_concept_details_new (id, concept_name, ts_code, name) ` +
			`SELECT id, concept_name, ts_code, name FROM (SELECT id, concept_name, ts_code, name, ` +
			`ROW_NUMBER() OVER (PARTITION BY id, ts_code ORDER BY concept_name, name) AS rn ` +
			`FROM stock_concept_details__staging) WHERE rn=1`,
		"DROP TABLE stock_concepts",
		"DROP TABLE stock_concept_details",
		"ALTER TABLE stock_concepts_new RENAME TO stock_concepts",
		"ALTER TABLE stock_concept_details_new RENAME TO stock_concept_details",
		"CREATE INDEX IF NOT EXISTS idx_concept_details_tscode ON stock_concept_details (ts_code)",
	}
	if err := e.DB.ExecTx(stmts); err != nil {
		return fmt.Errorf("概念发布失败已回滚: %w", err)
	}
	return nil
}
