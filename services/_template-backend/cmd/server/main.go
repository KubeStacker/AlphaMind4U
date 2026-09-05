// 新后端入口模板：仅健康检查，业务按需扩展。
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
)

func main() {
	addr := getenv("TEMPLATE_LISTEN_ADDR", "0.0.0.0:8002")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "template-backend"})
	})
	log.Printf("模板后端监听 %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

// getenv 读环境变量，空则回退默认值。
func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
