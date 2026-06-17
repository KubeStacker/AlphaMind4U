import logging
from collections.abc import Sequence

import pandas as pd


def dedupe_by_key(
    df: pd.DataFrame,
    *,
    key_cols: Sequence[str],
    table_name: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Keep the last row for duplicate primary keys before touching DuckDB ART indexes."""
    duplicate_rows = int(df.duplicated(subset=list(key_cols), keep=False).sum())
    if duplicate_rows:
        logger.warning(
            "%s incoming data contains %s duplicate primary-key rows; keeping the last row",
            table_name,
            duplicate_rows,
        )
    return df.drop_duplicates(subset=list(key_cols), keep="last")


def update_then_insert_missing(
    con,
    *,
    df: pd.DataFrame,
    table_name: str,
    view_name: str,
    columns: Sequence[str],
    key_cols: Sequence[str],
    logger: logging.Logger,
) -> None:
    """
    Upsert a DataFrame without DuckDB ON CONFLICT or delete/reinsert cycles.

    DuckDB primary keys are backed by ART indexes. In this project, bulk ON CONFLICT and
    deleting then reinserting the same date's keys have both been observed around ART
    invalidation failures. Updating non-key columns first and inserting only missing
    keys avoids duplicate key insertion into the ART index.
    """
    if df.empty:
        return

    cols = list(columns)
    keys = list(key_cols)
    update_cols = [col for col in cols if col not in keys]
    join_predicate = " AND ".join(f"t.{col} = v.{col}" for col in keys)
    col_csv = ",".join(cols)
    select_csv = ",".join(f"v.{col}" for col in cols)

    try:
        con.unregister(view_name)
    except Exception:
        pass

    transaction_started = False
    try:
        con.register(view_name, df)
        con.execute("BEGIN TRANSACTION")
        transaction_started = True

        if update_cols:
            set_clause = ", ".join(f"{col} = v.{col}" for col in update_cols)
            con.execute(
                f"""
                UPDATE {table_name} AS t
                SET {set_clause}
                FROM {view_name} AS v
                WHERE {join_predicate}
                """
            )

        con.execute(
            f"""
            INSERT INTO {table_name} ({col_csv})
            SELECT {select_csv}
            FROM {view_name} AS v
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name} AS t
                WHERE {join_predicate}
            )
            """
        )

        con.execute("COMMIT")
        transaction_started = False
    except Exception:
        if transaction_started:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
        raise
    finally:
        try:
            con.unregister(view_name)
        except Exception:
            pass
