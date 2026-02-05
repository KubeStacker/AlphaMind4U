# /backend/core/config.py

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    应用配置类，用于加载和管理环境变量。
    
    Attributes:
        tushare_token (str): Tushare 数据服务的 API Token。
    """
    
    # Tushare API Token, 从 .env 文件中读取
    # 访问 https://tushare.pro 注册以获取
    tushare_token: str = "YOUR_TUSHARE_TOKEN_HERE"

    # model_config 用于指定 .env 文件的位置
    # 在这个配置中，它会查找项目根目录下的 .env 文件
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8')

# 创建一个全局可用的配置实例
settings = Settings()

# 方便其他模块直接导入和使用
def get_settings():
    """
    返回全局配置实例。
    """
    return settings
