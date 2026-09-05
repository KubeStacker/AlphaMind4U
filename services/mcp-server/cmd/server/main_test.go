// LoadConfig 对客 Key 回落测试：缺省用共享令牌，两者皆无才报错。
package main

import (
	"testing"
)

// TestAPIKeyFallbackToStaticToken 未设 MCP Key 时回落到共享令牌。
func TestAPIKeyFallbackToStaticToken(t *testing.T) {
	t.Setenv("JARVIS_STATIC_AUTH_TOKEN", "shared-t")
	t.Setenv("JARVIS_MCP_API_KEY", "")
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("应成功: %v", err)
	}
	if cfg.APIKey != "shared-t" || cfg.StaticToken != "shared-t" {
		t.Fatalf("回落错误: %+v", cfg)
	}
}

// TestAPIKeyExplicitOverride 显式 MCP Key 优先于共享令牌。
func TestAPIKeyExplicitOverride(t *testing.T) {
	t.Setenv("JARVIS_STATIC_AUTH_TOKEN", "shared-t")
	t.Setenv("JARVIS_MCP_API_KEY", "mcp-only")
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("应成功: %v", err)
	}
	if cfg.APIKey != "mcp-only" {
		t.Fatalf("显式值应优先: %+v", cfg)
	}
}

// TestAPIKeyBothMissing 两者皆无启动失败。
func TestAPIKeyBothMissing(t *testing.T) {
	t.Setenv("JARVIS_STATIC_AUTH_TOKEN", "")
	t.Setenv("JARVIS_MCP_API_KEY", "")
	if _, err := LoadConfig(); err == nil {
		t.Fatalf("应报错")
	}
}
