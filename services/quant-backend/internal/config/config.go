// Package config 加载量化后端全部环境变量配置。
package config

import (
	"os"
	"strconv"
	"strings"
)

// Config 量化后端运行配置，由环境变量注入。
type Config struct {
	// TushareAPIURL 可选数据源地址，独立于凭据变量选择。
	TushareAPIURL string
	// ListenAddr 监听地址，如 0.0.0.0:8000。
	ListenAddr string
	// StaticToken /admin 接口 Bearer 令牌。
	StaticToken string
	// DBPath SQLite 库文件路径。
	DBPath string
	// TushareTokenType 短 token(short) 或长 token(long)。
	TushareTokenType string
	// ShortToken 短 token。
	ShortToken string
	// LongToken 长 token。
	LongToken string
	// EnableETLWorker 是否启动 ETL 任务消费者。
	EnableETLWorker bool
	// EnableScheduler 是否启动定时调度器。
	EnableScheduler bool
	// LogLevel 日志级别：debug/info/warn/error。
	LogLevel string
	// AdminUser 控制台管理员账号，单用户共用。
	AdminUser string
	// AdminPassword 控制台管理员密码，为空则登录接口停用。
	AdminPassword string
}

func getenv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func getbool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

// Load 从环境变量构建配置。
func Load() Config {
	return Config{
		TushareAPIURL:    strings.TrimSpace(os.Getenv("TUSHARE_API_URL")),
		ListenAddr:       getenv("QUANT_LISTEN_ADDR", "0.0.0.0:8000"),
		StaticToken:      strings.TrimSpace(os.Getenv("JARVIS_STATIC_AUTH_TOKEN")),
		DBPath:           getenv("QUANT_DB_PATH", "data/quant.db"),
		TushareTokenType: getenv("TUSHARE_TOKEN_TYPE", "long"),
		ShortToken:       strings.TrimSpace(os.Getenv("SHORT_TUSHARE_TOKEN")),
		LongToken:        strings.TrimSpace(os.Getenv("LONG_TUSHARE_TOKEN")),
		EnableETLWorker:  getbool("QUANT_ENABLE_ETL_WORKER", true),
		EnableScheduler:  getbool("QUANT_ENABLE_SCHEDULER", true),
		LogLevel:         getenv("QUANT_LOG_LEVEL", "info"),
		AdminUser:        getenv("JARVIS_ADMIN_USER", "admin"),
		AdminPassword:    strings.TrimSpace(os.Getenv("JARVIS_ADMIN_PASSWORD")),
	}
}

// TushareToken 按类型返回生效 token。
func (c Config) TushareToken() string {
	if c.TushareTokenType == "short" {
		return c.ShortToken
	}
	return c.LongToken
}

// IsShort 是否短 token 模式。
func (c Config) IsShort() bool { return c.TushareTokenType == "short" }
