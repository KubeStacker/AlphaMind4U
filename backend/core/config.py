# /backend/core/config.py

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    应用配置类，用于加载和管理环境变量。
    """
    
    # Tushare Token 类型: short 或 long
    tushare_token_type: str = "long"
    
    # Short token (需要设置 _DataApi__token 和 _DataApi__http_url)
    short_tushare_token: str = ""
    
    # Long token (标准token)
    long_tushare_token: str = ""
    
    # 静态服务令牌配置（用于 MCP 等无密码服务调用）
    jarvis_static_auth_token: str = ""
    jarvis_static_auth_username: str = "admin"

    # DuckDB 配置
    duckdb_memory_limit: str = "400MB"
    duckdb_threads: str = "2"
    duckdb_fatal_exit_enabled: bool = True
    duckdb_query_retries: int = 2
    duckdb_retry_delay_seconds: float = 1.0

    # 后端运行开关
    enable_etl_worker: bool = True
    enable_scheduler: bool = True
    
    @property
    def tushare_token(self) -> str:
        """根据token类型返回对应的token"""
        if self.tushare_token_type == "short":
            return self.short_tushare_token
        return self.long_tushare_token
    
    # model_config 用于指定 .env 文件的位置
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8')

# 创建一个全局可用的配置实例
settings = Settings()

# 方便其他模块直接导入和使用
def get_settings():
    """
    返回全局配置实例。
    """
    return settings
