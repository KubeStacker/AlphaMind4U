from etl.providers.base import DataProvider


class BaseTask:
    """ETL 任务基类。子类应使用 `logging.getLogger(__name__)` 获取独立 logger。"""

    def __init__(self, provider: DataProvider):
        self.provider = provider
