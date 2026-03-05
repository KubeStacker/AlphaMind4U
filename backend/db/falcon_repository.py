from __future__ import annotations

import json
from typing import Any

import pandas as pd

from db.connection import fetch_df, get_db_connection


CREATE_FALCON_SCHEMA_SQL = [
    "CREATE SEQUENCE IF NOT EXISTS falcon_runs_id_seq START 1;",
    """
    CREATE TABLE IF NOT EXISTS falcon_runs (
        run_id BIGINT PRIMARY KEY DEFAULT nextval('falcon_runs_id_seq'),
        strategy_id VARCHAR(80) NOT NULL,
        strategy_version INTEGER NOT NULL,
        trade_date DATE NOT NULL,
        params_json JSON,
        summary_json JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_deleted BOOLEAN DEFAULT FALSE,
        deleted_at TIMESTAMP,
        deleted_by VARCHAR(120)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS falcon_picks (
        run_id BIGINT NOT NULL,
        rank_no INTEGER NOT NULL,
        strategy_id VARCHAR(80) NOT NULL,
        trade_date DATE NOT NULL,
        ts_code VARCHAR(20) NOT NULL,
        name VARCHAR(80),
        strategy_score DOUBLE,
        confidence DOUBLE,
        signal_label VARCHAR(40),
        score_breakdown JSON,
        is_deleted BOOLEAN DEFAULT FALSE,
        PRIMARY KEY (run_id, rank_no)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS falcon_pick_eval (
        run_id BIGINT NOT NULL,
        strategy_id VARCHAR(80) NOT NULL,
        trade_date DATE NOT NULL,
        ts_code VARCHAR(20) NOT NULL,
        ret_5d DOUBLE,
        ret_10d DOUBLE,
        hit_5d BOOLEAN,
        hit_10d BOOLEAN,
        hold_days INTEGER,
        exit_reason VARCHAR(40),
        PRIMARY KEY (run_id, ts_code)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS falcon_strategy_state (
        strategy_id VARCHAR(80) PRIMARY KEY,
        version INTEGER NOT NULL,
        params_json JSON,
        note VARCHAR(200),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS falcon_strategy_daily_score (
        strategy_id VARCHAR(80) NOT NULL,
        trade_date DATE NOT NULL,
        score DOUBLE,
        details JSON,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (strategy_id, trade_date)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS falcon_evolution_log (
        id BIGINT PRIMARY KEY DEFAULT nextval('falcon_runs_id_seq'),
        strategy_id VARCHAR(80) NOT NULL,
        prev_version INTEGER,
        next_version INTEGER,
        prev_params JSON,
        next_params JSON,
        score_before DOUBLE,
        score_after DOUBLE,
        promoted BOOLEAN,
        details JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS falcon_op_log (
        id BIGINT PRIMARY KEY DEFAULT nextval('falcon_runs_id_seq'),
        strategy_id VARCHAR(80),
        op_type VARCHAR(40) NOT NULL,
        run_ids JSON,
        detail JSON,
        operator VARCHAR(120),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_falcon_runs_strategy_date ON falcon_runs (strategy_id, trade_date);",
    "CREATE INDEX IF NOT EXISTS idx_falcon_picks_run ON falcon_picks (run_id);",
    "CREATE INDEX IF NOT EXISTS idx_falcon_eval_run ON falcon_pick_eval (run_id);",
    "CREATE INDEX IF NOT EXISTS idx_falcon_op_strategy_time ON falcon_op_log (strategy_id, created_at);",
]


class DuckDbFalconRepository:
    def ensure_schema(self) -> None:
        with get_db_connection() as con:
            for sql in CREATE_FALCON_SCHEMA_SQL:
                con.execute(sql)
            # backward-compatible migration for existing tables
            con.execute("ALTER TABLE falcon_pick_eval ADD COLUMN IF NOT EXISTS hold_days INTEGER")
            con.execute("ALTER TABLE falcon_pick_eval ADD COLUMN IF NOT EXISTS exit_reason VARCHAR(40)")

    def get_strategy_state(self, strategy_id: str) -> dict[str, Any] | None:
        df = fetch_df(
            "SELECT strategy_id, version, params_json FROM falcon_strategy_state WHERE strategy_id = ?",
            [strategy_id],
        )
        if df.empty:
            return None
        row = df.iloc[0]
        params = row.get("params_json")
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except Exception:
                params = {}
        return {
            "strategy_id": str(row["strategy_id"]),
            "version": int(row.get("version", 1)),
            "params_json": params if isinstance(params, dict) else {},
        }

    def upsert_strategy_state(
        self,
        strategy_id: str,
        version: int,
        params_json: dict[str, Any],
        note: str,
    ) -> None:
        with get_db_connection() as con:
            con.execute(
                """
                INSERT INTO falcon_strategy_state (strategy_id, version, params_json, note, updated_at)
                VALUES (?, ?, ?, ?, NOW())
                ON CONFLICT (strategy_id) DO UPDATE
                SET version=excluded.version,
                    params_json=excluded.params_json,
                    note=excluded.note,
                    updated_at=NOW()
                """,
                [strategy_id, version, json.dumps(params_json, ensure_ascii=False), note],
            )

    def create_run(
        self,
        strategy_id: str,
        strategy_version: int,
        as_of_date: str,
        params_json: dict[str, Any],
    ) -> int:
        with get_db_connection() as con:
            row = con.execute(
                """
                INSERT INTO falcon_runs (strategy_id, strategy_version, trade_date, params_json)
                VALUES (?, ?, ?, ?)
                RETURNING run_id
                """,
                [strategy_id, strategy_version, as_of_date, json.dumps(params_json, ensure_ascii=False)],
            ).fetchone()
        return int(row[0])

    def complete_run(self, run_id: int, summary_json: dict[str, Any]) -> None:
        with get_db_connection() as con:
            con.execute(
                "UPDATE falcon_runs SET summary_json = ? WHERE run_id = ?",
                [json.dumps(summary_json, ensure_ascii=False), run_id],
            )

    def insert_picks(self, run_id: int, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with get_db_connection() as con:
            for r in rows:
                con.execute(
                    """
                    INSERT INTO falcon_picks (
                        run_id, rank_no, strategy_id, trade_date, ts_code, name,
                        strategy_score, confidence, signal_label, score_breakdown
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        r["rank_no"],
                        r["strategy_id"],
                        r["trade_date"],
                        r["ts_code"],
                        r.get("name"),
                        r.get("strategy_score"),
                        r.get("confidence"),
                        r.get("signal_label"),
                        json.dumps(r.get("score_breakdown", {}), ensure_ascii=False),
                    ],
                )

    def upsert_eval_rows(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with get_db_connection() as con:
            for r in rows:
                con.execute(
                    """
                    INSERT INTO falcon_pick_eval (
                        run_id, strategy_id, trade_date, ts_code, ret_5d, ret_10d, hit_5d, hit_10d, hold_days, exit_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (run_id, ts_code) DO UPDATE SET
                        ret_5d=excluded.ret_5d,
                        ret_10d=excluded.ret_10d,
                        hit_5d=excluded.hit_5d,
                        hit_10d=excluded.hit_10d,
                        hold_days=excluded.hold_days,
                        exit_reason=excluded.exit_reason
                    """,
                    [
                        r["run_id"],
                        r["strategy_id"],
                        r["trade_date"],
                        r["ts_code"],
                        r.get("ret_5d"),
                        r.get("ret_10d"),
                        r.get("hit_5d"),
                        r.get("hit_10d"),
                        r.get("hold_days"),
                        r.get("exit_reason"),
                    ],
                )

    def upsert_strategy_daily_score(self, strategy_id: str, trade_date: str, score: float, details: dict[str, Any]) -> None:
        with get_db_connection() as con:
            con.execute(
                """
                INSERT INTO falcon_strategy_daily_score (strategy_id, trade_date, score, details)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (strategy_id, trade_date) DO UPDATE SET
                    score=excluded.score,
                    details=excluded.details,
                    updated_at=NOW()
                """,
                [strategy_id, trade_date, score, json.dumps(details, ensure_ascii=False)],
            )

    def list_runs(self, strategy_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        lim = max(1, min(200, int(limit)))
        if strategy_id:
            df = fetch_df(
                f"""
                SELECT run_id, strategy_id, strategy_version, trade_date, summary_json, created_at
                FROM falcon_runs
                WHERE is_deleted = FALSE AND strategy_id = ?
                ORDER BY created_at DESC
                LIMIT {lim}
                """,
                [strategy_id],
            )
        else:
            df = fetch_df(
                f"""
                SELECT run_id, strategy_id, strategy_version, trade_date, summary_json, created_at
                FROM falcon_runs
                WHERE is_deleted = FALSE
                ORDER BY created_at DESC
                LIMIT {lim}
                """,
            )
        rows = []
        for _, r in df.iterrows():
            summary = r.get("summary_json")
            if isinstance(summary, str):
                try:
                    summary = json.loads(summary)
                except Exception:
                    summary = {}
            rows.append(
                {
                    "run_id": int(r["run_id"]),
                    "strategy_id": str(r["strategy_id"]),
                    "strategy_version": int(r["strategy_version"]),
                    "trade_date": str(r["trade_date"])[:10],
                    "summary": summary if isinstance(summary, dict) else {},
                    "created_at": str(r["created_at"]),
                }
            )
        return rows

    def list_deleted_runs(self, strategy_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        lim = max(1, min(200, int(limit)))
        if strategy_id:
            df = fetch_df(
                f"""
                SELECT run_id, strategy_id, strategy_version, trade_date, summary_json, created_at, deleted_at, deleted_by
                FROM falcon_runs
                WHERE is_deleted = TRUE AND strategy_id = ?
                ORDER BY deleted_at DESC NULLS LAST, created_at DESC
                LIMIT {lim}
                """,
                [strategy_id],
            )
        else:
            df = fetch_df(
                f"""
                SELECT run_id, strategy_id, strategy_version, trade_date, summary_json, created_at, deleted_at, deleted_by
                FROM falcon_runs
                WHERE is_deleted = TRUE
                ORDER BY deleted_at DESC NULLS LAST, created_at DESC
                LIMIT {lim}
                """,
            )
        rows = []
        for _, r in df.iterrows():
            summary = r.get("summary_json")
            if isinstance(summary, str):
                try:
                    summary = json.loads(summary)
                except Exception:
                    summary = {}
            rows.append(
                {
                    "run_id": int(r["run_id"]),
                    "strategy_id": str(r["strategy_id"]),
                    "strategy_version": int(r["strategy_version"]),
                    "trade_date": str(r["trade_date"])[:10],
                    "summary": summary if isinstance(summary, dict) else {},
                    "created_at": str(r["created_at"]),
                    "deleted_at": str(r.get("deleted_at") or ""),
                    "deleted_by": str(r.get("deleted_by") or ""),
                }
            )
        return rows

    def get_run_detail(self, run_id: int) -> dict[str, Any] | None:
        run_df = fetch_df(
            """
            SELECT run_id, strategy_id, strategy_version, trade_date, params_json, summary_json, created_at
            FROM falcon_runs
            WHERE run_id = ? AND is_deleted = FALSE
            LIMIT 1
            """,
            [run_id],
        )
        if run_df.empty:
            return None

        picks = fetch_df(
            """
            SELECT p.rank_no, p.ts_code, p.name, p.strategy_score, p.confidence, p.signal_label, p.score_breakdown,
                   e.ret_5d, e.ret_10d, e.hit_5d, e.hit_10d, e.hold_days, e.exit_reason
            FROM falcon_picks p
            LEFT JOIN falcon_pick_eval e ON p.run_id = e.run_id AND p.ts_code = e.ts_code
            WHERE p.run_id = ? AND p.is_deleted = FALSE
            ORDER BY p.rank_no ASC
            """,
            [run_id],
        )

        run_row = run_df.iloc[0]
        params = run_row.get("params_json")
        summary = run_row.get("summary_json")
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except Exception:
                params = {}
        if isinstance(summary, str):
            try:
                summary = json.loads(summary)
            except Exception:
                summary = {}

        pick_rows = []
        for _, p in picks.iterrows():
            breakdown = p.get("score_breakdown")
            if isinstance(breakdown, str):
                try:
                    breakdown = json.loads(breakdown)
                except Exception:
                    breakdown = {}
            pick_rows.append(
                {
                    "rank_no": int(p["rank_no"]),
                    "ts_code": str(p["ts_code"]),
                    "name": str(p.get("name") or ""),
                    "strategy_score": float(p.get("strategy_score") or 0),
                    "confidence": float(p.get("confidence") or 0),
                    "signal_label": str(p.get("signal_label") or "观察"),
                    "score_breakdown": breakdown if isinstance(breakdown, dict) else {},
                    "ret_5d": None if pd.isna(p.get("ret_5d")) else float(p.get("ret_5d")),
                    "ret_10d": None if pd.isna(p.get("ret_10d")) else float(p.get("ret_10d")),
                    "hit_5d": None if pd.isna(p.get("hit_5d")) else bool(p.get("hit_5d")),
                    "hit_10d": None if pd.isna(p.get("hit_10d")) else bool(p.get("hit_10d")),
                    "hold_days": None if pd.isna(p.get("hold_days")) else int(p.get("hold_days")),
                    "exit_reason": str(p.get("exit_reason") or ""),
                }
            )

        return {
            "run_id": int(run_row["run_id"]),
            "strategy_id": str(run_row["strategy_id"]),
            "strategy_version": int(run_row["strategy_version"]),
            "trade_date": str(run_row["trade_date"])[:10],
            "params": params if isinstance(params, dict) else {},
            "summary": summary if isinstance(summary, dict) else {},
            "created_at": str(run_row["created_at"]),
            "picks": pick_rows,
        }

    def soft_delete_run(self, run_id: int, operator: str) -> bool:
        with get_db_connection() as con:
            row = con.execute("SELECT strategy_id FROM falcon_runs WHERE run_id = ?", [run_id]).fetchone()
            if not row:
                return False
            strategy_id = str(row[0])
            con.execute(
                """
                UPDATE falcon_runs
                SET is_deleted = TRUE, deleted_at = CURRENT_TIMESTAMP, deleted_by = ?
                WHERE run_id = ?
                """,
                [operator, run_id],
            )
            con.execute("UPDATE falcon_picks SET is_deleted = TRUE WHERE run_id = ?", [run_id])
            self._record_op(
                con=con,
                strategy_id=strategy_id,
                op_type="soft_delete",
                run_ids=[run_id],
                detail={},
                operator=operator,
            )
        return True

    def restore_run(self, run_id: int) -> bool:
        with get_db_connection() as con:
            row = con.execute("SELECT strategy_id FROM falcon_runs WHERE run_id = ?", [run_id]).fetchone()
            if not row:
                return False
            strategy_id = str(row[0])
            con.execute(
                """
                UPDATE falcon_runs
                SET is_deleted = FALSE, deleted_at = NULL, deleted_by = NULL
                WHERE run_id = ?
                """,
                [run_id],
            )
            con.execute("UPDATE falcon_picks SET is_deleted = FALSE WHERE run_id = ?", [run_id])
            self._record_op(
                con=con,
                strategy_id=strategy_id,
                op_type="restore",
                run_ids=[run_id],
                detail={},
                operator="api",
            )
        return True

    def restore_runs(self, run_ids: list[int]) -> int:
        ids = [int(x) for x in run_ids if isinstance(x, int) or str(x).isdigit()]
        if not ids:
            return 0
        count = 0
        with get_db_connection() as con:
            for run_id in ids:
                exists = con.execute("SELECT COUNT(*) FROM falcon_runs WHERE run_id = ?", [run_id]).fetchone()[0]
                if not exists:
                    continue
                con.execute(
                    """
                    UPDATE falcon_runs
                    SET is_deleted = FALSE, deleted_at = NULL, deleted_by = NULL
                    WHERE run_id = ?
                    """,
                    [run_id],
                )
                con.execute("UPDATE falcon_picks SET is_deleted = FALSE WHERE run_id = ?", [run_id])
                strategy_id_row = con.execute("SELECT strategy_id FROM falcon_runs WHERE run_id = ?", [run_id]).fetchone()
                strategy_id = str(strategy_id_row[0]) if strategy_id_row else None
                self._record_op(
                    con=con,
                    strategy_id=strategy_id,
                    op_type="restore_batch",
                    run_ids=[run_id],
                    detail={},
                    operator="api",
                )
                count += 1
        return count

    def hard_delete_run(self, run_id: int) -> bool:
        with get_db_connection() as con:
            row = con.execute("SELECT strategy_id FROM falcon_runs WHERE run_id = ?", [run_id]).fetchone()
            if not row:
                return False
            strategy_id = str(row[0])
            con.execute("DELETE FROM falcon_pick_eval WHERE run_id = ?", [run_id])
            con.execute("DELETE FROM falcon_picks WHERE run_id = ?", [run_id])
            con.execute("DELETE FROM falcon_runs WHERE run_id = ?", [run_id])
            self._record_op(
                con=con,
                strategy_id=strategy_id,
                op_type="hard_delete",
                run_ids=[run_id],
                detail={},
                operator="api",
            )
        return True

    def hard_delete_runs(self, run_ids: list[int]) -> int:
        ids = [int(x) for x in run_ids if isinstance(x, int) or str(x).isdigit()]
        if not ids:
            return 0
        count = 0
        with get_db_connection() as con:
            for run_id in ids:
                exists = con.execute("SELECT COUNT(*) FROM falcon_runs WHERE run_id = ?", [run_id]).fetchone()[0]
                if not exists:
                    continue
                con.execute("DELETE FROM falcon_pick_eval WHERE run_id = ?", [run_id])
                con.execute("DELETE FROM falcon_picks WHERE run_id = ?", [run_id])
                con.execute("DELETE FROM falcon_runs WHERE run_id = ?", [run_id])
                self._record_op(
                    con=con,
                    strategy_id=None,
                    op_type="hard_delete_batch",
                    run_ids=[run_id],
                    detail={},
                    operator="api",
                )
                count += 1
        return count

    def soft_delete_runs_by_range(
        self,
        strategy_id: str,
        start_date: str,
        end_date: str,
        operator: str,
    ) -> int:
        with get_db_connection() as con:
            run_ids_df = con.execute(
                """
                SELECT run_id
                FROM falcon_runs
                WHERE is_deleted = FALSE
                  AND strategy_id = ?
                  AND trade_date BETWEEN ? AND ?
                """,
                [strategy_id, start_date, end_date],
            ).fetchdf()
            if run_ids_df.empty:
                return 0
            run_ids = [int(x) for x in run_ids_df["run_id"].tolist()]
            for run_id in run_ids:
                con.execute(
                    """
                    UPDATE falcon_runs
                    SET is_deleted = TRUE, deleted_at = CURRENT_TIMESTAMP, deleted_by = ?
                    WHERE run_id = ?
                    """,
                    [operator, run_id],
                )
                con.execute("UPDATE falcon_picks SET is_deleted = TRUE WHERE run_id = ?", [run_id])
            self._record_op(
                con=con,
                strategy_id=strategy_id,
                op_type="soft_delete_range",
                run_ids=run_ids,
                detail={"start_date": start_date, "end_date": end_date},
                operator=operator,
            )
            return len(run_ids)

    def list_operation_logs(self, strategy_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        lim = max(1, min(200, int(limit)))
        if strategy_id:
            df = fetch_df(
                f"""
                SELECT strategy_id, op_type, run_ids, detail, operator, created_at
                FROM falcon_op_log
                WHERE strategy_id = ?
                ORDER BY created_at DESC
                LIMIT {lim}
                """,
                [strategy_id],
            )
        else:
            df = fetch_df(
                f"""
                SELECT strategy_id, op_type, run_ids, detail, operator, created_at
                FROM falcon_op_log
                ORDER BY created_at DESC
                LIMIT {lim}
                """
            )
        rows = []
        for _, r in df.iterrows():
            run_ids = r.get("run_ids")
            detail = r.get("detail")
            if isinstance(run_ids, str):
                try:
                    run_ids = json.loads(run_ids)
                except Exception:
                    run_ids = []
            if isinstance(detail, str):
                try:
                    detail = json.loads(detail)
                except Exception:
                    detail = {}
            rows.append(
                {
                    "strategy_id": str(r.get("strategy_id") or ""),
                    "op_type": str(r.get("op_type") or ""),
                    "run_ids": run_ids if isinstance(run_ids, list) else [],
                    "detail": detail if isinstance(detail, dict) else {},
                    "operator": str(r.get("operator") or ""),
                    "created_at": str(r.get("created_at") or ""),
                }
            )
        return rows

    def get_strategy_daily_score(self, strategy_id: str, trade_date: str) -> dict[str, Any] | None:
        df = fetch_df(
            """
            SELECT strategy_id, trade_date, score, details, updated_at
            FROM falcon_strategy_daily_score
            WHERE strategy_id = ? AND trade_date = ?
            LIMIT 1
            """,
            [strategy_id, trade_date],
        )
        if df.empty:
            return None
        row = df.iloc[0]
        details = row.get("details")
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except Exception:
                details = {}
        return {
            "strategy_id": str(row["strategy_id"]),
            "trade_date": str(row["trade_date"])[:10],
            "score": None if pd.isna(row.get("score")) else float(row.get("score")),
            "details": details if isinstance(details, dict) else {},
            "updated_at": str(row.get("updated_at")),
        }

    def get_history_eval_for_strategy(self, strategy_id: str, lookback_runs: int = 120) -> pd.DataFrame:
        lim = max(20, min(500, int(lookback_runs)))
        query = """
        WITH latest_runs AS (
            SELECT run_id
            FROM falcon_runs
            WHERE strategy_id = ? AND is_deleted = FALSE
            ORDER BY created_at DESC
            LIMIT ?
        )
        SELECT p.run_id, p.rank_no, e.ret_10d
        FROM falcon_picks p
        JOIN falcon_pick_eval e ON p.run_id = e.run_id AND p.ts_code = e.ts_code
        WHERE p.run_id IN (SELECT run_id FROM latest_runs)
          AND p.is_deleted = FALSE
        """
        return fetch_df(query, [strategy_id, lim])

    def record_evolution(
        self,
        strategy_id: str,
        prev_version: int,
        next_version: int,
        prev_params: dict[str, Any],
        next_params: dict[str, Any],
        score_before: float,
        score_after: float,
        promoted: bool,
        details: dict[str, Any],
    ) -> None:
        with get_db_connection() as con:
            con.execute(
                """
                INSERT INTO falcon_evolution_log (
                    strategy_id, prev_version, next_version, prev_params, next_params,
                    score_before, score_after, promoted, details
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    strategy_id,
                    prev_version,
                    next_version,
                    json.dumps(prev_params, ensure_ascii=False),
                    json.dumps(next_params, ensure_ascii=False),
                    score_before,
                    score_after,
                    promoted,
                    json.dumps(details, ensure_ascii=False),
                ],
            )

    def _record_op(
        self,
        con,
        strategy_id: str | None,
        op_type: str,
        run_ids: list[int],
        detail: dict[str, Any],
        operator: str,
    ) -> None:
        con.execute(
            """
            INSERT INTO falcon_op_log (strategy_id, op_type, run_ids, detail, operator)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                strategy_id,
                op_type,
                json.dumps(run_ids, ensure_ascii=False),
                json.dumps(detail, ensure_ascii=False),
                operator,
            ],
        )
