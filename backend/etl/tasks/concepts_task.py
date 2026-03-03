from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection
from core.constants import CONCEPT_BLACKLIST
from etl.config import settings
import pandas as pd
import time

class ConceptsTask(BaseTask):
    def sync(self):
        """同步概念 - 仅使用THS同花顺概念(需要6000积分)"""
        if settings.tushare_token_type != 'short':
            self.logger.info("概念同步仅支持短token，跳过")
            return
            
        self.logger.info("开始同步THS同花顺概念...")
        
        self._clear_concepts()
        
        self._sync_ths_concepts()
        
        self.logger.info("THS概念同步完成")

    def _sync_ths_concepts(self) -> bool:
        """同步THS同花顺概念 - 需要6000积分"""
        try:
            df_ths = self.provider.ths_index(type='N')
            if df_ths.empty:
                self.logger.warning("获取同花顺概念列表失败，可能无权限(需要6000积分)")
                return False
            
            df_ths = df_ths[~df_ths['name'].isin(CONCEPT_BLACKLIST)]
            self.logger.info(f"开始同步THS同花顺概念，共 {len(df_ths)} 个")
            
            self._sync_ths_details(df_ths)
            return True
        except Exception as e:
            error_msg = str(e)
            if '权限' in error_msg or '无权限' in error_msg or '积分' in error_msg:
                self.logger.warning(f"THS概念同步无权限(需要6000积分): {error_msg[:50]}")
            else:
                self.logger.error(f"同步THS概念失败: {e}")
            return False

    def _sync_ths_details(self, df_ths):
        count = 0
        total = 0
        
        for idx, row in df_ths.iterrows():
            concept_code = row['ts_code']
            concept_name = row['name']
            
            if concept_name in CONCEPT_BLACKLIST:
                continue
            
            try:
                df_detail = self.provider.ths_member(ts_code=concept_code)
                if not df_detail.empty:
                    df_detail = df_detail[~df_detail['con_name'].isin(CONCEPT_BLACKLIST)]
                    
                    if not df_detail.empty:
                        ths_detail = pd.DataFrame({
                            'id': concept_code,
                            'concept_name': concept_name + '_THS',
                            'ts_code': df_detail['con_code'],
                            'name': df_detail['con_name']
                        })
                        
                        with get_db_connection() as con:
                            con.register('ths_view', ths_detail)
                            con.execute("""
                                INSERT INTO stock_concept_details (id, concept_name, ts_code, name) 
                                SELECT id, concept_name, ts_code, name FROM ths_view 
                                ON CONFLICT DO NOTHING
                            """)
                        total += len(ths_detail)
            except Exception as e:
                self.logger.debug(f"同步 {concept_name} 失败")
            
            count += 1
            if count % 20 == 0:
                self.logger.info(f"THS概念进度: {count}/{len(df_ths)}, 已插入 {total} 条")
                time.sleep(1)
            else:
                time.sleep(0.3)
        
        self.logger.info(f"THS概念明细同步完成: {count} 个概念, {total} 条记录")

    def sync_stock_concepts(self, ts_code: str):
        """同步指定股票的概念信息"""
        if settings.tushare_token_type != 'short':
            self.logger.info("概念同步仅支持短token，跳过")
            return
            
        all_concepts = self.provider.concept()
        all_concepts = all_concepts[~all_concepts['name'].isin(CONCEPT_BLACKLIST)]
        
        found = []
        
        for idx, row in all_concepts.iterrows():
            concept_code = row['code']
            concept_name = row['name']
            
            try:
                df_detail = self.provider.concept_detail(id=concept_code)
                if ts_code in df_detail['ts_code'].values:
                    stock_name = df_detail[df_detail['ts_code'] == ts_code]['name'].values[0]
                    found.append({
                        'id': concept_code,
                        'concept_name': concept_name,
                        'ts_code': ts_code,
                        'name': stock_name
                    })
            except Exception as e:
                pass
            
            if idx % 50 == 0:
                time.sleep(0.3)
        
        if found:
            df_stock_concepts = pd.DataFrame(found)
            with get_db_connection() as con:
                con.register('df_view', df_stock_concepts)
                con.execute("""
                    INSERT INTO stock_concept_details (id, concept_name, ts_code, name) 
                    SELECT id, concept_name, ts_code, name FROM df_view 
                    ON CONFLICT DO NOTHING
                """)
        
        self.logger.info(f"{ts_code} 概念同步完成: {len(found)} 个")

    def _clear_concepts(self):
        try:
            with get_db_connection() as con:
                con.execute('DELETE FROM stock_concept_details')
                con.execute('DELETE FROM stock_concepts')
            self.logger.info("已清空概念数据")
        except Exception as e:
            self.logger.warning(f"清空概念数据失败: {e}")
