// 迁移执行器：按文件名顺序执行内嵌 SQL，schema_version 记账。
package db

import (
	"embed"
	"fmt"
	"sort"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Migrate 执行未应用的迁移，返回已应用版本号。
func (d *DB) Migrate() ([]int, error) {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	var applied []int
	for _, name := range names {
		var ver int
		if _, err := fmt.Sscanf(name, "%04d_", &ver); err != nil {
			return applied, fmt.Errorf("迁移文件名非法: %s", name)
		}
		done, err := d.QueryInt("SELECT COUNT(*) FROM schema_version WHERE version=?", ver)
		if err != nil {
			return applied, err
		}
		if done > 0 {
			continue
		}
		body, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return applied, err
		}
		if err := d.ExecTx(splitStmts(string(body))); err != nil {
			return applied, fmt.Errorf("迁移 %s 失败: %w", name, err)
		}
		if _, err := d.Exec("INSERT INTO schema_version (version) VALUES (?)", ver); err != nil {
			return applied, err
		}
		applied = append(applied, ver)
	}
	return applied, nil
}
