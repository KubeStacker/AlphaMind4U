"""Initialize only raw collection and ETL operational tables."""

import logging
import os

from db.connection import DATABASE_PATH, get_db_connection
from db.schema import ALL_TABLES_SQL


logger = logging.getLogger(__name__)


def _has_primary_key(con, table_name: str) -> bool:
    return any(bool(row[5]) for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall())


def _rebuild_table_without_art_indexes(
    con, *, table_name: str, key_cols: list[str], index_names: list[str]
) -> None:
    """Remove ART constraints from high-churn fact tables while preserving rows."""
    table_info = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    if not table_info:
        return
    for index_name in index_names:
        con.execute(f"DROP INDEX IF EXISTS {index_name}")
    if not _has_primary_key(con, table_name):
        return

    columns = [row[1] for row in table_info]
    if any(key not in columns for key in key_cols):
        return
    quoted_cols = ", ".join(f'"{column}"' for column in columns)
    partition_cols = ", ".join(f'"{column}"' for column in key_cols)
    temporary = f"{table_name}__no_art"
    con.execute(f"DROP TABLE IF EXISTS {temporary}")
    con.execute(
        f"""
        CREATE TABLE {temporary} AS
        SELECT {quoted_cols}
        FROM (
            SELECT {quoted_cols},
                   ROW_NUMBER() OVER (
                       PARTITION BY {partition_cols} ORDER BY rowid DESC
                   ) AS __rn
            FROM {table_name}
        )
        WHERE __rn = 1
        """
    )
    con.execute(f"DROP TABLE {table_name}")
    con.execute(f"ALTER TABLE {temporary} RENAME TO {table_name}")
    logger.info("%s 已迁移为无 ART 主键的数据表", table_name)


def _migrate_high_churn_tables(con) -> None:
    tables = (
        ("daily_price", ["trade_date", "ts_code"], ["idx_daily_price_tscode", "idx_daily_price_date"]),
        ("stock_moneyflow", ["ts_code", "trade_date"], ["idx_moneyflow_date"]),
        ("stock_daily_basic", ["trade_date", "ts_code"], ["idx_stock_daily_basic_date", "idx_stock_daily_basic_tscode"]),
        ("market_index", ["trade_date", "ts_code"], []),
        ("stock_margin", ["ts_code", "trade_date"], ["idx_margin_date", "idx_stock_margin_date", "idx_stock_margin_tscode"]),
        ("stock_suspend", ["ts_code", "trade_date"], []),
        ("index_weight", ["index_code", "con_code", "trade_date"], []),
        ("moneyflow_hsgt", ["trade_date"], []),
    )
    for table_name, key_cols, index_names in tables:
        try:
            _rebuild_table_without_art_indexes(
                con,
                table_name=table_name,
                key_cols=key_cols,
                index_names=index_names,
            )
        except Exception as exc:
            logger.error("%s ART 索引迁移失败: %s", table_name, exc)


def _migrate_existing_columns(con) -> None:
    """Safely add new columns to pre-existing DuckDB tables."""
    column_migrations = [
        ("stock_basic", "delist_date", "DATE"),
        ("stock_basic", "exchange", "VARCHAR(10)"),
        ("stock_basic", "cnspell", "VARCHAR(20)"),
        ("stock_income", "rd_exp", "DOUBLE"),
        ("stock_fina_indicator", "dt_eps", "DOUBLE"),
        ("stock_fina_indicator", "roe_waa", "DOUBLE"),
        ("stock_fina_indicator", "roe_dt", "DOUBLE"),
        ("stock_fina_indicator", "roa", "DOUBLE"),
        ("stock_fina_indicator", "debt_to_assets", "DOUBLE"),
        ("stock_fina_indicator", "current_ratio", "DOUBLE"),
        ("stock_fina_indicator", "quick_ratio", "DOUBLE"),
        ("stock_fina_indicator", "ocfps", "DOUBLE"),
        ("stock_fina_indicator", "cfps", "DOUBLE"),
        ("stock_fina_indicator", "or_yoy", "DOUBLE"),
        ("stock_fina_indicator", "op_yoy", "DOUBLE"),
        ("stock_fina_indicator", "ebt_yoy", "DOUBLE"),
        ("stock_fina_indicator", "rd_exp", "DOUBLE"),
        ("stock_fina_indicator", "update_flag", "VARCHAR(10)"),
    ]
    for table, col, col_type in column_migrations:
        try:
            con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {col_type}")
        except Exception as exc:
            logger.debug("添加字段 %s.%s 异常(可能表未创建): %s", table, col, exc)


def initialize_database() -> None:
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    logger.info("正在初始化原始采集数据表")
    with get_db_connection() as con:
        for table_sql in ALL_TABLES_SQL:
            con.execute(table_sql)
        _migrate_existing_columns(con)
        _migrate_high_churn_tables(con)

        # 兼容已有数据库；不删除旧业务表或历史数据。
    logger.info("原始采集数据表初始化完成")


if __name__ == "__main__":
    initialize_database()
