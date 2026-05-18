from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection
from core.constants import CONCEPT_BLACKLIST
from core.config import settings
import pandas as pd
import time

STAGING_CONCEPTS_TABLE = "stock_concepts__staging"
STAGING_CONCEPT_DETAILS_TABLE = "stock_concept_details__staging"
PUBLISH_CONCEPTS_TABLE = "stock_concepts__publish"
PUBLISH_CONCEPT_DETAILS_TABLE = "stock_concept_details__publish"
BACKUP_CONCEPTS_TABLE = "stock_concepts__backup"
BACKUP_CONCEPT_DETAILS_TABLE = "stock_concept_details__backup"


class ConceptsTask(BaseTask):
    def _unregister_view(self, con, view_name: str):
        try:
            con.unregister(view_name)
        except Exception:
            pass

    def sync(self):
        """同步概念分类与成分股，优先 THS，无法使用时回退到 Tushare concept 接口。"""
        self.logger.info("开始同步概念数据...")
        self._prepare_staging_tables()

        try:
            synced = False

            if settings.tushare_token_type == "short":
                synced = self._sync_ths_concepts()
                if synced:
                    self.logger.info("THS 同花顺概念已写入 staging，准备发布")
                else:
                    self.logger.warning("THS 概念不可用，回退到 Tushare concept 接口")

            if not synced:
                synced = self._sync_ts_concepts()

            if synced:
                self._publish_staged_concepts()
                self.logger.info("概念数据已原子化发布")
            else:
                self.logger.warning("概念同步未获取到有效数据，保留现有库内数据")
        finally:
            self._cleanup_staging_tables()

    def _prepare_staging_tables(self):
        with get_db_connection() as con:
            con.execute(f"DROP TABLE IF EXISTS {STAGING_CONCEPTS_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {STAGING_CONCEPT_DETAILS_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {PUBLISH_CONCEPTS_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {PUBLISH_CONCEPT_DETAILS_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {BACKUP_CONCEPTS_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {BACKUP_CONCEPT_DETAILS_TABLE}")
            con.execute(
                f"CREATE TABLE {STAGING_CONCEPTS_TABLE} AS "
                "SELECT code, name, src FROM stock_concepts WHERE 1=0"
            )
            con.execute(
                f"CREATE TABLE {STAGING_CONCEPT_DETAILS_TABLE} AS "
                "SELECT id, concept_name, ts_code, name FROM stock_concept_details WHERE 1=0"
            )

    def _cleanup_staging_tables(self):
        with get_db_connection() as con:
            con.execute(f"DROP TABLE IF EXISTS {STAGING_CONCEPTS_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {STAGING_CONCEPT_DETAILS_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {PUBLISH_CONCEPTS_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {PUBLISH_CONCEPT_DETAILS_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {BACKUP_CONCEPTS_TABLE}")
            con.execute(f"DROP TABLE IF EXISTS {BACKUP_CONCEPT_DETAILS_TABLE}")

    def _normalize_concept_catalog_df(self, df_concepts: pd.DataFrame) -> pd.DataFrame:
        normalized = df_concepts[["code", "name", "src"]].copy()
        normalized["code"] = normalized["code"].fillna("").astype(str).str.strip()
        normalized["name"] = normalized["name"].fillna("").astype(str).str.strip()
        normalized["src"] = normalized["src"].fillna("").astype(str).str.strip()

        invalid_count = int((normalized["code"] == "").sum())
        if invalid_count:
            self.logger.warning(f"概念目录存在 {invalid_count} 条空 code 记录，已跳过")
            normalized = normalized[normalized["code"] != ""].copy()

        duplicate_count = int(normalized.duplicated(subset=["code"]).sum())
        if duplicate_count:
            self.logger.warning(
                f"概念目录存在 {duplicate_count} 条重复 code 记录，已按 code 去重"
            )
            normalized = normalized.drop_duplicates(subset=["code"], keep="first")

        return normalized.reset_index(drop=True)

    def _normalize_concept_detail_df(self, df_details: pd.DataFrame) -> pd.DataFrame:
        normalized = df_details[["id", "concept_name", "ts_code", "name"]].copy()
        normalized["id"] = normalized["id"].fillna("").astype(str).str.strip()
        normalized["concept_name"] = (
            normalized["concept_name"].fillna("").astype(str).str.strip()
        )
        normalized["ts_code"] = normalized["ts_code"].fillna("").astype(str).str.strip()
        normalized["name"] = normalized["name"].fillna("").astype(str).str.strip()

        invalid_mask = (normalized["id"] == "") | (normalized["ts_code"] == "")
        invalid_count = int(invalid_mask.sum())
        if invalid_count:
            self.logger.warning(f"概念明细存在 {invalid_count} 条空主键记录，已跳过")
            normalized = normalized[~invalid_mask].copy()

        duplicate_count = int(normalized.duplicated(subset=["id", "ts_code"]).sum())
        if duplicate_count:
            self.logger.warning(
                f"概念明细存在 {duplicate_count} 条重复主键记录，已按 (id, ts_code) 去重"
            )
            normalized = normalized.sort_values(
                ["id", "ts_code", "concept_name", "name"], kind="stable"
            ).drop_duplicates(subset=["id", "ts_code"], keep="first")

        return normalized.reset_index(drop=True)

    def _replace_concept_catalog(
        self,
        df_concepts: pd.DataFrame,
        table_name: str = STAGING_CONCEPTS_TABLE,
    ):
        if df_concepts is None or df_concepts.empty:
            raise ValueError("概念目录为空，拒绝覆盖现有概念数据")

        df_concepts = self._normalize_concept_catalog_df(df_concepts)
        if df_concepts.empty:
            raise ValueError("概念目录清洗后为空，拒绝覆盖现有概念数据")

        with get_db_connection() as con:
            view_name = "concept_catalog_view"
            self._unregister_view(con, view_name)
            try:
                con.execute(f"DELETE FROM {table_name}")
                con.register(view_name, df_concepts)
                con.execute(
                    f"""
                    INSERT INTO {table_name} (code, name, src)
                    SELECT code, name, src
                    FROM {view_name}
                    """
                )
            finally:
                self._unregister_view(con, view_name)
        self.logger.info(f"已写入 staging 概念目录: {len(df_concepts)} 条")

    def _insert_concept_details(
        self,
        df_details: pd.DataFrame,
        table_name: str = STAGING_CONCEPT_DETAILS_TABLE,
        view_name: str = "concept_detail_view",
    ):
        if df_details is None or df_details.empty:
            return

        df_details = self._normalize_concept_detail_df(df_details)
        if df_details.empty:
            return

        with get_db_connection() as con:
            self._unregister_view(con, view_name)
            try:
                con.execute(f"DELETE FROM {table_name} WHERE 1=0")
                con.register(view_name, df_details)
                con.execute(
                    f"""
                    INSERT INTO {table_name} (id, concept_name, ts_code, name)
                    SELECT id, concept_name, ts_code, name
                    FROM {view_name} v
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {table_name} t
                        WHERE t.id = v.id AND t.ts_code = v.ts_code
                    )
                    """
                )
            finally:
                self._unregister_view(con, view_name)

    def _flush_concept_details_batch(
        self,
        buffered_frames: list[pd.DataFrame],
        table_name: str,
        view_name: str,
    ) -> int:
        if not buffered_frames:
            return 0

        merged_df = pd.concat(buffered_frames, ignore_index=True)
        self._insert_concept_details(
            merged_df,
            table_name=table_name,
            view_name=view_name,
        )
        inserted = len(merged_df)
        buffered_frames.clear()
        return inserted

    def _publish_staged_concepts(self):
        with get_db_connection() as con:
            catalog_count = con.execute(
                f"SELECT COUNT(*) FROM {STAGING_CONCEPTS_TABLE}"
            ).fetchone()[0]
            detail_count = con.execute(
                f"SELECT COUNT(*) FROM {STAGING_CONCEPT_DETAILS_TABLE}"
            ).fetchone()[0]
            duplicate_detail_keys = con.execute(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT id, ts_code
                    FROM {STAGING_CONCEPT_DETAILS_TABLE}
                    GROUP BY 1, 2
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]

            if catalog_count <= 0 or detail_count <= 0:
                raise ValueError("staging 概念数据不完整，拒绝覆盖现有库")
            if duplicate_detail_keys:
                self.logger.warning(
                    f"staging 概念明细存在 {duplicate_detail_keys} 组重复主键，先清理重复再发布"
                )
                con.execute(
                    f"""
                    CREATE TABLE {STAGING_CONCEPT_DETAILS_TABLE}_cleaned AS
                    SELECT id, concept_name, ts_code, name
                    FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (PARTITION BY id, ts_code ORDER BY concept_name, name) AS rn
                        FROM {STAGING_CONCEPT_DETAILS_TABLE}
                    )
                    WHERE rn = 1
                    """
                )
                con.execute(f"DROP TABLE {STAGING_CONCEPT_DETAILS_TABLE}")
                con.execute(
                    f"ALTER TABLE {STAGING_CONCEPT_DETAILS_TABLE}_cleaned RENAME TO {STAGING_CONCEPT_DETAILS_TABLE}"
                )
                con.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_staging_concept_details ON {STAGING_CONCEPT_DETAILS_TABLE} (id, ts_code)"
                )

            try:
                with get_db_connection() as con:
                    con.execute("DROP INDEX IF EXISTS idx_concept_details_tscode")

                    con.execute("""
                        CREATE TABLE IF NOT EXISTS stock_concepts_new AS 
                        SELECT code, name, src FROM stock_concepts WHERE 1=0
                    """)
                    con.execute("""
                        CREATE TABLE IF NOT EXISTS stock_concept_details_new AS 
                        SELECT id, concept_name, ts_code, name FROM stock_concept_details WHERE 1=0
                    """)

                    con.execute("""
                        INSERT INTO stock_concepts_new (code, name, src)
                        SELECT code, name, src
                        FROM (
                            SELECT code, name, src,
                                ROW_NUMBER() OVER (PARTITION BY code ORDER BY name, src) AS rn
                            FROM stock_concepts__staging
                        ) catalog_dedup
                        WHERE rn = 1
                    """)

                    con.execute("""
                        INSERT INTO stock_concept_details_new (id, concept_name, ts_code, name)
                        SELECT id, concept_name, ts_code, name
                        FROM (
                            SELECT id, concept_name, ts_code, name,
                                ROW_NUMBER() OVER (PARTITION BY id, ts_code ORDER BY concept_name, name) AS rn
                            FROM stock_concept_details__staging
                        ) detail_dedup
                        WHERE rn = 1
                    """)

                    con.execute("DROP TABLE IF EXISTS stock_concepts")
                    con.execute("DROP TABLE IF EXISTS stock_concept_details")
                    con.execute(
                        "ALTER TABLE stock_concepts_new RENAME TO stock_concepts"
                    )
                    con.execute(
                        "ALTER TABLE stock_concept_details_new RENAME TO stock_concept_details"
                    )

                    con.execute("ALTER TABLE stock_concepts ADD PRIMARY KEY (code)")
                    con.execute(
                        "ALTER TABLE stock_concept_details ADD PRIMARY KEY (id, ts_code)"
                    )

                    con.execute(
                        "CREATE INDEX IF NOT EXISTS idx_concept_details_tscode ON stock_concept_details (ts_code)"
                    )

                    self.logger.info("概念数据已原子化发布")
            except Exception as e:
                self.logger.error(f"概念发布失败: {e}")
                with get_db_connection() as con:
                    con.execute("DROP TABLE IF EXISTS stock_concepts_new")
                    con.execute("DROP TABLE IF EXISTS stock_concept_details_new")
                raise

    def _sync_ths_concepts(self) -> bool:
        """同步THS同花顺概念 - 需要6000积分"""
        try:
            df_ths = self.provider.ths_index(type="N")
            if df_ths.empty:
                self.logger.warning("获取同花顺概念列表失败，可能无权限(需要6000积分)")
                return False

            df_ths = df_ths[~df_ths["name"].isin(CONCEPT_BLACKLIST)].copy()
            if df_ths.empty:
                self.logger.warning("THS 概念目录为空")
                return False

            catalog_df = pd.DataFrame(
                {
                    "code": df_ths["ts_code"],
                    "name": df_ths["name"],
                    "src": "ths",
                }
            )
            self._replace_concept_catalog(catalog_df)
            self._clear_concept_details(STAGING_CONCEPT_DETAILS_TABLE)

            self.logger.info(f"开始同步THS同花顺概念，共 {len(df_ths)} 个")
            self._sync_ths_details(df_ths)
            return True
        except Exception as e:
            error_msg = str(e)
            if "权限" in error_msg or "无权限" in error_msg or "积分" in error_msg:
                self.logger.warning(
                    f"THS概念同步无权限(需要6000积分): {error_msg[:50]}"
                )
            else:
                self.logger.error(f"同步THS概念失败: {e}")
            return False

    def _sync_ths_details(self, df_ths):
        count = 0
        total = 0
        detail_batches = []

        for _, row in df_ths.iterrows():
            concept_code = row["ts_code"]
            concept_name = row["name"]

            if concept_name in CONCEPT_BLACKLIST:
                continue

            try:
                df_detail = self.provider.ths_member(ts_code=concept_code)
                if not df_detail.empty:
                    df_detail = df_detail[
                        ~df_detail["con_name"].isin(CONCEPT_BLACKLIST)
                    ]

                    if not df_detail.empty:
                        ths_detail = pd.DataFrame(
                            {
                                "id": concept_code,
                                "concept_name": concept_name + "_THS",
                                "ts_code": df_detail["con_code"],
                                "name": df_detail["con_name"],
                            }
                        )
                        detail_batches.append(ths_detail)
            except Exception:
                self.logger.debug(f"同步 {concept_name} 失败")

            count += 1
            if detail_batches and count % 25 == 0:
                total += self._flush_concept_details_batch(
                    detail_batches,
                    table_name=STAGING_CONCEPT_DETAILS_TABLE,
                    view_name="ths_view",
                )
            if count % 20 == 0:
                self.logger.info(
                    f"THS概念进度: {count}/{len(df_ths)}, 已插入 {total} 条"
                )
                time.sleep(1)
            else:
                time.sleep(0.3)

        total += self._flush_concept_details_batch(
            detail_batches,
            table_name=STAGING_CONCEPT_DETAILS_TABLE,
            view_name="ths_view",
        )
        self.logger.info(f"THS概念明细同步完成: {count} 个概念, {total} 条记录")

    def sync_stock_concepts(self, ts_code: str):
        """同步指定股票的概念信息"""
        all_concepts = self.provider.concept()
        if all_concepts is None or all_concepts.empty:
            self.logger.warning("概念目录为空，无法同步单只股票概念")
            return
        all_concepts = all_concepts[~all_concepts["name"].isin(CONCEPT_BLACKLIST)]

        found = []

        for idx, row in all_concepts.iterrows():
            concept_code = row["code"]
            concept_name = row["name"]

            try:
                df_detail = self.provider.concept_detail(id=concept_code)
                if ts_code in df_detail["ts_code"].values:
                    stock_name = df_detail[df_detail["ts_code"] == ts_code][
                        "name"
                    ].values[0]
                    found.append(
                        {
                            "id": concept_code,
                            "concept_name": concept_name,
                            "ts_code": ts_code,
                            "name": stock_name,
                        }
                    )
            except Exception:
                pass

            if idx % 50 == 0:
                time.sleep(0.3)

        if found:
            df_stock_concepts = pd.DataFrame(found)
            self._insert_concept_details(
                df_stock_concepts,
                table_name="stock_concept_details",
                view_name="df_view",
            )

        self.logger.info(f"{ts_code} 概念同步完成: {len(found)} 个")

    def _clear_concepts(self):
        try:
            with get_db_connection() as con:
                con.execute("DELETE FROM stock_concept_details")
                con.execute("DELETE FROM stock_concepts")
            self.logger.info("已清空概念数据")
        except Exception as e:
            self.logger.warning(f"清空概念数据失败: {e}")

    def _sync_ts_concepts(self) -> bool:
        """使用 Tushare concept/concept_detail 同步概念数据，兼容 long token。"""
        try:
            df_concepts = self.provider.concept()
            if df_concepts is None or df_concepts.empty:
                self.logger.warning("Tushare concept 目录为空")
                return False

            df_concepts = df_concepts[
                ~df_concepts["name"].isin(CONCEPT_BLACKLIST)
            ].copy()
            if df_concepts.empty:
                self.logger.warning("Tushare concept 目录过滤黑名单后为空")
                return False

            catalog_df = df_concepts[["code", "name", "src"]].copy()
            self._replace_concept_catalog(catalog_df)
            self._clear_concept_details(STAGING_CONCEPT_DETAILS_TABLE)

            total = 0
            count = 0
            detail_batches = []
            for _, row in df_concepts.iterrows():
                concept_code = row["code"]
                concept_name = row["name"]

                try:
                    df_detail = self.provider.concept_detail(id=concept_code)
                    if df_detail is None or df_detail.empty:
                        continue

                    df_detail = df_detail[
                        ~df_detail["concept_name"].isin(CONCEPT_BLACKLIST)
                    ].copy()
                    if df_detail.empty:
                        continue

                    detail_batches.append(df_detail)
                except Exception as e:
                    self.logger.debug(f"同步 {concept_name}({concept_code}) 失败: {e}")

                count += 1
                if detail_batches and count % 25 == 0:
                    total += self._flush_concept_details_batch(
                        detail_batches,
                        table_name=STAGING_CONCEPT_DETAILS_TABLE,
                        view_name="concept_detail_view",
                    )
                if count % 50 == 0:
                    self.logger.info(
                        f"Tushare 概念进度: {count}/{len(df_concepts)}, 已插入 {total} 条"
                    )
                time.sleep(0.1)

            total += self._flush_concept_details_batch(
                detail_batches,
                table_name=STAGING_CONCEPT_DETAILS_TABLE,
                view_name="concept_detail_view",
            )
            self.logger.info(
                f"Tushare 概念明细同步完成: {count} 个概念, {total} 条记录"
            )
            return total > 0
        except Exception as e:
            self.logger.error(f"Tushare 概念同步失败: {e}")
            return False

    def _clear_concept_details(self, table_name: str = STAGING_CONCEPT_DETAILS_TABLE):
        try:
            with get_db_connection() as con:
                con.execute(f"DELETE FROM {table_name}")
            self.logger.info(f"已清空概念明细数据: {table_name}")
        except Exception as e:
            self.logger.warning(f"清空概念明细失败: {e}")
