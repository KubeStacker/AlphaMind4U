"""
集中式日志配置模块

功能:
- TimedRotatingFileHandler: 按天轮转, 保留 30 天
- StreamHandler: 保持 docker logs 可见
- 按模块分级过滤
- 上海时区时间戳
- ERROR 级别单独记录到 error log
"""

import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import pytz

# 上海时区
SHANGHAI_TZ = pytz.timezone("Asia/Shanghai")

# 日志目录 (相对于 backend/)
LOG_DIR = Path(os.getenv("JARVIS_LOG_DIR", Path(__file__).resolve().parent.parent / "logs"))

# 模块日志级别配置
MODULE_LOG_LEVELS: dict[str, int] = {
    "apscheduler": logging.WARNING,  # 调度器心跳噪音大
    "apscheduler.scheduler": logging.WARNING,
    "apscheduler.executors": logging.WARNING,
}


class ShanghaiFormatter(logging.Formatter):
    """上海时区日志格式化器"""

    def formatTime(self, record, datefmt=None):
        import datetime as dt

        tz_dt = dt.datetime.fromtimestamp(record.created, tz=SHANGHAI_TZ)
        if datefmt:
            return tz_dt.strftime(datefmt)
        return tz_dt.strftime("%Y-%m-%d %H:%M:%S")


LOG_FORMAT = "%(asctime)s [%(levelname)-5s] [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class ErrorOnlyFilter(logging.Filter):
    """只允许 ERROR 及以上级别通过"""

    def filter(self, record):
        return record.levelno >= logging.ERROR


def setup_logging(
    log_dir: Path | None = None,
    root_level: int = logging.INFO,
    retention_days: int = 30,
) -> None:
    """初始化集中式日志系统.

    Args:
        log_dir: 日志目录, 默认 backend/logs/
        root_level: 根日志级别
        retention_days: 日志保留天数
    """
    if log_dir is None:
        log_dir = LOG_DIR

    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        log_dir = Path("/tmp/jarvis-logs")
        log_dir.mkdir(parents=True, exist_ok=True)

    # --- 清除已有 handlers (避免重复) ---
    root = logging.root
    for h in list(root.handlers):
        root.removeHandler(h)

    # --- 1. 控制台输出 (docker logs) ---
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ShanghaiFormatter(LOG_FORMAT, DATE_FORMAT))
    console_handler.setLevel(root_level)

    # --- 2. 每日轮转文件 (全量) ---
    daily_file = log_dir / "jarvis.log"
    file_handler = TimedRotatingFileHandler(
        filename=str(daily_file),
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(ShanghaiFormatter(LOG_FORMAT, DATE_FORMAT))
    file_handler.setLevel(root_level)

    # --- 3. ERROR 单独文件 ---
    error_file = log_dir / "jarvis.error.log"
    error_handler = TimedRotatingFileHandler(
        filename=str(error_file),
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
    )
    error_handler.suffix = "%Y-%m-%d"
    error_handler.setFormatter(ShanghaiFormatter(LOG_FORMAT, DATE_FORMAT))
    error_handler.setLevel(logging.ERROR)
    error_handler.addFilter(ErrorOnlyFilter())

    # --- 装配 ---
    root.setLevel(root_level)
    root.addHandler(console_handler)
    root.addHandler(file_handler)
    root.addHandler(error_handler)

    # --- 模块级别覆盖 ---
    for module, level in MODULE_LOG_LEVELS.items():
        logging.getLogger(module).setLevel(level)

    logger = logging.getLogger(__name__)
    logger.info("日志系统初始化完成: log_dir=%s, retention=%d天", log_dir, retention_days)


def get_log_files(log_dir: Path | None = None) -> list[dict]:
    """列出可用日志文件.

    Returns:
        [{"name": "...", "size": 12345, "date": "2026-08-03"}, ...]
    """
    if log_dir is None:
        log_dir = LOG_DIR

    if not log_dir.exists():
        return []

    files = []
    for f in sorted(log_dir.iterdir(), reverse=True):
        if f.is_file() and f.suffix in (".log",) or any(
            f.name.endswith(ext) for ext in (".log",)
        ):
            stat = f.stat()
            files.append(
                {
                    "name": f.name,
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                }
            )
    return files


def read_log_file(
    filename: str,
    lines: int = 200,
    log_dir: Path | None = None,
) -> str:
    """读取日志文件最近的 N 行.

    Args:
        filename: 日志文件名
        lines: 读取行数
        log_dir: 日志目录

    Returns:
        日志内容字符串
    """
    import subprocess

    if log_dir is None:
        log_dir = LOG_DIR

    filepath = log_dir / filename
    if not filepath.exists():
        raise FileNotFoundError(f"日志文件不存在: {filename}")

    # 使用 tail 高效读取
    try:
        result = subprocess.run(
            ["tail", "-n", str(lines), str(filepath)],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout
    except Exception:
        # 回退到 Python 方式
        with open(filepath, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
            return "".join(all_lines[-lines:])
