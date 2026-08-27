"""受控的原始采集数据只读 API。"""

from __future__ import annotations

import math
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from db.connection import fetch_df, get_db_connection


router = APIRouter(prefix="/datasets", tags=["Datasets"])


DATASETS: dict[str, dict[str, Any]] = {
    "stock_basic": {
        "description": "股票基础信息",
        "date_column": "list_date",
        "excluded_columns": ("pinyin", "pinyin_abbr"),
    },
    "daily_price": {
        "description": "股票日线行情",
        "date_column": "trade_date",
        "excluded_columns": ("factors",),
    },
    "stock_concepts": {"description": "概念分类", "date_column": None},
    "stock_concept_details": {"description": "概念成分", "date_column": None},
    "stock_moneyflow": {
        "description": "个股资金流向",
        "date_column": "trade_date",
        "excluded_columns": ("net_mf_ratio",),
    },
    "market_index": {"description": "市场指数日线", "date_column": "trade_date"},
    "stock_margin": {"description": "融资融券明细", "date_column": "trade_date"},
    "stock_income": {"description": "利润表", "date_column": "end_date"},
    "stock_balance_sheet": {"description": "资产负债表", "date_column": "end_date"},
    "stock_cashflow": {"description": "现金流量表", "date_column": "end_date"},
    "stock_fina_indicator": {"description": "财务指标", "date_column": "end_date"},
    "stock_daily_basic": {"description": "日频基础指标", "date_column": "trade_date"},
    "stock_index_member_all": {"description": "申万行业成分", "date_column": "in_date"},
    "stock_express": {"description": "业绩快报", "date_column": "end_date"},
    "stock_suspend": {"description": "每日停复牌信息", "date_column": "trade_date"},
    "stock_namechange": {"description": "股票更名历史", "date_column": "start_date"},
    "stock_dividend": {"description": "分红送股数据", "date_column": "end_date"},
    "index_weight": {"description": "指数成分及权重", "date_column": "trade_date"},
    "stock_forecast": {"description": "业绩预告", "date_column": "end_date"},
    "moneyflow_hsgt": {"description": "沪深港通资金流向", "date_column": "trade_date"},
}


class DatasetQuery(BaseModel):
    dataset: str
    columns: list[str] | None = None
    filters: dict[str, Any] = Field(default_factory=dict)
    start_date: str | None = None
    end_date: str | None = None
    order_by: str | None = None
    descending: bool = False
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0, le=1_000_000)


def _dataset_config(name: str) -> dict[str, Any]:
    config = DATASETS.get(name)
    if config is None:
        raise HTTPException(status_code=404, detail=f"不允许读取数据集: {name}")
    return config


def _columns(name: str) -> list[dict[str, Any]]:
    _dataset_config(name)
    with get_db_connection() as con:
        rows = con.execute(f'PRAGMA table_info("{name}")').fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"数据表尚未初始化: {name}")
    excluded = set(DATASETS[name].get("excluded_columns", ()))
    return [
        {
            "name": row[1],
            "type": row[2],
            "nullable": not bool(row[3]),
            "primary_key": bool(row[5]),
        }
        for row in rows
        if row[1] not in excluded
    ]


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "item"):
        return _json_value(value.item())
    return value


@router.get("")
def list_datasets():
    """列出 MCP 允许读取的原始采集数据表。"""
    return {
        "datasets": [
            {
                "name": name,
                "description": config["description"],
                "date_column": config["date_column"],
            }
            for name, config in DATASETS.items()
        ]
    }


@router.get("/{name}/schema")
def describe_dataset(name: str):
    """返回单个采集数据表的字段定义。"""
    config = _dataset_config(name)
    return {
        "name": name,
        "description": config["description"],
        "date_column": config["date_column"],
        "columns": _columns(name),
    }


@router.post("/query")
def query_dataset(request: DatasetQuery):
    """对一个白名单采集表执行参数化、限量的只读查询。"""
    config = _dataset_config(request.dataset)
    schema = _columns(request.dataset)
    available = {column["name"] for column in schema}

    selected = request.columns or [column["name"] for column in schema]
    invalid = sorted(set(selected) - available)
    if invalid:
        raise HTTPException(status_code=400, detail=f"未知字段: {', '.join(invalid)}")
    if not selected:
        raise HTTPException(status_code=400, detail="columns 不能为空")

    invalid_filters = sorted(set(request.filters) - available)
    if invalid_filters:
        raise HTTPException(status_code=400, detail=f"未知过滤字段: {', '.join(invalid_filters)}")

    date_column = config["date_column"]
    if (request.start_date or request.end_date) and not date_column:
        raise HTTPException(status_code=400, detail="该数据集没有日期范围字段")

    order_by = request.order_by or date_column or selected[0]
    if order_by not in available:
        raise HTTPException(status_code=400, detail=f"未知排序字段: {order_by}")

    where: list[str] = []
    params: list[Any] = []
    for column, value in request.filters.items():
        if value is None:
            where.append(f'"{column}" IS NULL')
        else:
            where.append(f'"{column}" = ?')
            params.append(value)
    if request.start_date:
        where.append(f'"{date_column}" >= ?')
        params.append(request.start_date)
    if request.end_date:
        where.append(f'"{date_column}" <= ?')
        params.append(request.end_date)

    quoted_columns = ", ".join(f'"{column}"' for column in selected)
    sql = f'SELECT {quoted_columns} FROM "{request.dataset}"'
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f' ORDER BY "{order_by}" {"DESC" if request.descending else "ASC"} LIMIT ? OFFSET ?'
    params.extend([request.limit, request.offset])

    frame = fetch_df(sql, params=params)
    frame = frame.astype(object).where(pd.notnull(frame), None)
    rows = [
        {key: _json_value(value) for key, value in row.items()}
        for row in frame.to_dict("records")
    ]
    del frame
    return {
        "dataset": request.dataset,
        "columns": selected,
        "rows": rows,
        "count": len(rows),
        "limit": request.limit,
        "offset": request.offset,
    }
