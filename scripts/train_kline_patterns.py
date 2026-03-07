from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from db.connection import fetch_df  # noqa: E402
from etl.utils.kline_patterns import (  # noqa: E402
    evaluate_pattern_performance,
    save_pattern_calibration,
    train_pattern_calibration,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="训练经典K线形态校准文件")
    parser.add_argument("--start-date", default=None, help="起始日期，如 2024-01-01")
    parser.add_argument("--end-date", default=None, help="结束日期，如 2026-03-07")
    parser.add_argument(
        "--output",
        default=str(BACKEND_DIR / "etl" / "utils" / "kline_pattern_calibration.json"),
        help="校准文件输出路径",
    )
    parser.add_argument(
        "--horizons",
        default="3,5,10",
        help="未来收益窗口，逗号分隔，例如 3,5,10",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.55,
        help="纳入训练的最低原始置信度",
    )
    parser.add_argument(
        "--positive-return-threshold",
        type=float,
        default=0.0,
        help="判定命中的收益阈值，例如 0.01 表示未来涨/跌超1%",
    )
    return parser.parse_args()


def load_daily_bars(start_date: str | None, end_date: str | None):
    conditions = []
    params: list[object] = []

    if start_date:
        conditions.append("trade_date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("trade_date <= ?")
        params.append(end_date)

    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"""
        SELECT trade_date, ts_code, open, high, low, close, pct_chg, vol, amount
        FROM daily_price
        {where_sql}
        ORDER BY ts_code, trade_date
    """
    return fetch_df(sql, params)


def main() -> int:
    args = parse_args()
    horizons = tuple(int(x.strip()) for x in args.horizons.split(",") if x.strip())
    if not horizons:
        raise ValueError("至少指定一个 horizon")

    df = load_daily_bars(args.start_date, args.end_date)
    if df.empty:
        print("未查询到 daily_price 历史数据，无法训练。")
        return 1

    calibration = train_pattern_calibration(
        df=df,
        group_col="ts_code",
        date_col="trade_date",
        horizons=horizons,
        min_confidence=args.min_confidence,
        positive_return_threshold=args.positive_return_threshold,
    )
    output_path = save_pattern_calibration(calibration, args.output)

    summary = evaluate_pattern_performance(
        df=df,
        group_col="ts_code",
        date_col="trade_date",
        horizons=horizons,
        min_confidence=args.min_confidence,
        positive_return_threshold=args.positive_return_threshold,
    )

    print(f"已输出校准文件: {output_path}")
    if summary.empty:
        print("未生成有效评估摘要。")
        return 0

    primary = horizons[0]
    show_cols = [
        "pattern",
        "direction",
        "sample_count",
        "avg_confidence",
        f"hit_rate_{primary}d",
        f"avg_ret_{primary}d",
        f"directional_edge_{primary}d",
    ]
    show_cols = [c for c in show_cols if c in summary.columns]
    print(summary[show_cols].head(12).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
