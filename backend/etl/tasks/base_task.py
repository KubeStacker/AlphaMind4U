from etl.providers.base import DataProvider
import logging

logger = logging.getLogger(__name__)

class BaseTask:
    def __init__(self, provider: DataProvider):
        self.provider = provider
        self.logger = logger
