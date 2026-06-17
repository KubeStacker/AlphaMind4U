# /backend/db/init_db.py

import os
from db.connection import get_db_connection, DATABASE_PATH
from db.schema import ALL_TABLES_SQL


def _migrate_watchlist_schema(con):
    """
    将旧版全局 watchlist 表迁移为按 user_id 隔离的新结构。
    旧版自选是全局共享的；迁移时复制给当前所有已有用户，避免任一用户原有自选丢失。
    """
    watchlist_info = con.execute("PRAGMA table_info('watchlist')").fetchall()
    if not watchlist_info:
        return

    columns = {row[1] for row in watchlist_info}
    if "user_id" in columns:
        return

    print("检测到旧版 watchlist 表，开始迁移为按用户隔离结构...")
    con.execute("ALTER TABLE watchlist RENAME TO watchlist_legacy")
    con.execute(
        """
        CREATE TABLE watchlist (
            user_id         INTEGER NOT NULL,
            ts_code         VARCHAR(15) NOT NULL,
            name            VARCHAR(50),
            remark          VARCHAR(255),
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, ts_code)
        )
        """
    )

    user_rows = con.execute("SELECT id FROM users ORDER BY id").fetchall()
    if user_rows:
        for user_id, in user_rows:
            con.execute(
                """
                INSERT INTO watchlist (user_id, ts_code, name, remark, created_at)
                SELECT ?, ts_code, name, remark, created_at
                FROM watchlist_legacy
                """,
                (user_id,),
            )

    con.execute("DROP TABLE watchlist_legacy")
    print("watchlist 表迁移完成：已切换为 user_id + ts_code 复合主键。")


def _backfill_user_ai_provider_configs(con):
    """
    将旧版 user_ai_config 中的当前 provider 配置回填到按 provider 拆分的新表。
    仅在目标 provider 尚未存在配置时写入，避免覆盖用户已保存的新结构数据。
    """
    rows = con.execute(
        """
        SELECT
            user_id,
            COALESCE(NULLIF(model_provider, ''), 'openai') AS provider,
            model_name,
            api_key,
            base_url,
            system_prompt,
            COALESCE(max_tokens, 1200) AS max_tokens,
            COALESCE(temperature, 0.35) AS temperature
        FROM user_ai_config
        """
    ).fetchall()

    migrated = 0
    for row in rows:
        user_id, provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature = row
        exists = con.execute(
            "SELECT 1 FROM user_ai_provider_configs WHERE user_id = ? AND provider = ?",
            (user_id, provider),
        ).fetchone()
        if exists:
            continue
        con.execute(
            """
            INSERT INTO user_ai_provider_configs (
                user_id, provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature),
        )
        migrated += 1

    print(f"AI provider 配置回填完成：新增 {migrated} 条历史配置。")


def _has_primary_key(con, table_name: str) -> bool:
    info = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return any(bool(row[5]) for row in info)


def _drop_indexes(con, index_names: list[str]) -> None:
    for index_name in index_names:
        try:
            con.execute(f"DROP INDEX IF EXISTS {index_name}")
        except Exception as exc:
            print(f"删除索引 {index_name} 失败: {exc}")


def _rebuild_table_without_art_indexes(
    con,
    *,
    table_name: str,
    key_cols: list[str],
    index_names: list[str],
) -> None:
    """
    Rebuild high-churn fact tables without DuckDB ART-backed PK/indexes.

    Several daily sync tables are append/update-heavy. Once an ART index is invalidated,
    any later write can fatal the process even if application code avoids ON CONFLICT.
    Rebuilding with CTAS drops constraints/indexes and deduplicates by the logical key.
    """
    table_info = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    if not table_info:
        return

    _drop_indexes(con, index_names)

    if not _has_primary_key(con, table_name):
        return

    columns = [row[1] for row in table_info]
    missing_keys = [col for col in key_cols if col not in columns]
    if missing_keys:
        print(f"跳过 {table_name} ART 迁移，缺少主键列: {missing_keys}")
        return

    quoted_cols = ", ".join(f'"{col}"' for col in columns)
    partition_cols = ", ".join(f'"{col}"' for col in key_cols)
    tmp_table = f"{table_name}__no_art"

    print(f"检测到 {table_name} 存在 ART 主键，开始重建为无主键去重表...")
    con.execute(f"DROP TABLE IF EXISTS {tmp_table}")
    con.execute(
        f"""
        CREATE TABLE {tmp_table} AS
        SELECT {quoted_cols}
        FROM (
            SELECT
                {quoted_cols},
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_cols}
                    ORDER BY rowid DESC
                ) AS __rn
            FROM {table_name}
        )
        WHERE __rn = 1
        """
    )
    con.execute(f"DROP TABLE {table_name}")
    con.execute(f"ALTER TABLE {tmp_table} RENAME TO {table_name}")
    print(f"{table_name} ART 主键迁移完成。")


def _migrate_art_indexed_fact_tables(con):
    fact_tables = [
        {
            "table_name": "daily_price",
            "key_cols": ["trade_date", "ts_code"],
            "index_names": ["idx_daily_price_tscode", "idx_daily_price_date"],
        },
        {
            "table_name": "stock_moneyflow",
            "key_cols": ["ts_code", "trade_date"],
            "index_names": ["idx_moneyflow_date"],
        },
        {
            "table_name": "stock_daily_basic",
            "key_cols": ["trade_date", "ts_code"],
            "index_names": ["idx_stock_daily_basic_date", "idx_stock_daily_basic_tscode"],
        },
        {
            "table_name": "stock_factor_daily",
            "key_cols": ["trade_date", "ts_code"],
            "index_names": ["idx_stock_factor_daily_date", "idx_stock_factor_daily_tscode"],
        },
        {
            "table_name": "market_index",
            "key_cols": ["trade_date", "ts_code"],
            "index_names": [],
        },
        {
            "table_name": "stock_margin",
            "key_cols": ["ts_code", "trade_date"],
            "index_names": ["idx_margin_date", "idx_stock_margin_date", "idx_stock_margin_tscode"],
        },
    ]
    for table in fact_tables:
        try:
            _rebuild_table_without_art_indexes(con, **table)
        except Exception as exc:
            print(f"{table['table_name']} ART 索引迁移失败: {exc}")


def initialize_database():
    """
    初始化数据库。如果数据库文件不存在，则创建它，并根据 schema.py 中的定义创建所有表。
    """
    # 确保数据库所在目录存在
    db_dir = os.path.dirname(DATABASE_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"创建 '{db_dir}' 目录。")
    
    print("正在连接数据库并初始化表结构...")
    try:
        with get_db_connection() as con:
            for table_sql in ALL_TABLES_SQL:
                con.execute(table_sql)
            print("数据库表结构初始化/验证完成。")

            _migrate_art_indexed_fact_tables(con)
            
            # 检查 users 表是否为空，如果为空则添加默认管理员用户
            user_count = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            if user_count == 0:
                print("users 表为空，正在添加默认管理员 admin/admin ...")
                # 在实际应用中，密码应该是强哈希值
                # 这里为了简化，使用明文，但 schema 中字段是 hashed_password
                # 我们将在用户管理部分实现真正的哈希
                from passlib.context import CryptContext
                pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
                admin_password_hash = pwd_context.hash("admin")
                con.execute(
                    "INSERT INTO users (username, hashed_password, role) VALUES (?, ?, ?)",
                    ("admin", admin_password_hash, "admin")
                )
                print("默认管理员 'admin' 添加成功。")

            _migrate_watchlist_schema(con)
            
            # 清理历史脏数据 (针对用户提到的 20260101/0102 幻觉数据)
            con.execute("DELETE FROM daily_price WHERE trade_date IN ('2026-01-01', '2026-01-02')")
            con.execute("DELETE FROM market_sentiment WHERE trade_date IN ('2026-01-01', '2026-01-02')")
            print("已自动清理 2026-01-01/02 的异常占位数据。")
            
            # 添加缺失的列到现有表
            try:
                # 为 user_ai_config 表添加 selected_template_id 列
                con.execute("ALTER TABLE user_ai_config ADD COLUMN IF NOT EXISTS selected_template_id INTEGER")
                print("已添加 user_ai_config.selected_template_id 列")
            except Exception as e:
                print(f"添加 user_ai_config.selected_template_id 列失败: {e}")

            try:
                _backfill_user_ai_provider_configs(con)
            except Exception as e:
                print(f"回填 user_ai_provider_configs 失败: {e}")
            
            try:
                # 为 stock_moneyflow 表添加 net_mf_ratio 列
                con.execute("ALTER TABLE stock_moneyflow ADD COLUMN IF NOT EXISTS net_mf_ratio DOUBLE")
                print("已添加 stock_moneyflow.net_mf_ratio 列")
            except Exception as e:
                print(f"添加 stock_moneyflow.net_mf_ratio 列失败: {e}")
            
            try:
                # 为 stock_basic 表添加拼音相关列
                con.execute("ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS pinyin VARCHAR(100)")
                con.execute("ALTER TABLE stock_basic ADD COLUMN IF NOT EXISTS pinyin_abbr VARCHAR(20)")
                print("已添加 stock_basic.pinyin 和 pinyin_abbr 列")
            except Exception as e:
                print(f"添加 stock_basic 拼音列失败: {e}")

            try:
                # 为 watchlist 表添加 sort_order 列
                con.execute("ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS sort_order INTEGER DEFAULT 0")
                # 根据 created_at 初始化已有数据的 sort_order
                con.execute("""
                    UPDATE watchlist SET sort_order = sub.rn
                    FROM (
                        SELECT user_id, ts_code,
                               ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS rn
                        FROM watchlist WHERE sort_order = 0
                    ) AS sub
                    WHERE watchlist.user_id = sub.user_id AND watchlist.ts_code = sub.ts_code
                """)
                print("已添加 watchlist.sort_order 列")
            except Exception as e:
                print(f"添加 watchlist.sort_order 列失败: {e}")

    except Exception as e:
        print(f"数据库初始化失败: {e}")

if __name__ == "__main__":
    # 当直接运行此脚本时，执行初始化
    initialize_database()
