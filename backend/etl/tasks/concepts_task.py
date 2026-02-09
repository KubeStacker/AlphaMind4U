from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection
import pandas as pd
import time

class ConceptsTask(BaseTask):
    def sync(self):
        self.logger.info("开始同步概念分类...")
        try:
            df_concept = self.provider.concept()
            if not df_concept.empty:
                with get_db_connection() as con:
                    con.execute("INSERT INTO stock_concepts SELECT * FROM df_concept ON CONFLICT (code) DO UPDATE SET name=excluded.name, src=excluded.src")
                
                # 同步明细 (仅 Tushare 支持较好，Akshare 需适配)
                if self.provider.provider_name == 'tushare':
                    self._sync_details(df_concept)
        except Exception as e:
            self.logger.error(f"同步概念列表失败: {e}")

    def _sync_details(self, df_concept):
        count = 0
        for index, row in df_concept.iterrows():
            concept_id = row['code']
            concept_name = row['name']
            
            try:
                df_detail = self.provider.concept_detail(id=concept_id)
                if not df_detail.empty:
                    processed_detail = df_detail[['id', 'concept_name', 'ts_code', 'name']]
                    with get_db_connection() as con:
                        con.execute("INSERT INTO stock_concept_details SELECT * FROM processed_detail ON CONFLICT DO NOTHING")
                
                count += 1
                if count % 10 == 0:
                    time.sleep(2.0)
                else:
                    time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"同步概念明细 {concept_name} 失败: {e}")
