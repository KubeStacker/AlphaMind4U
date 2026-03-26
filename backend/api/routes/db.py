# /backend/api/routes/db.py

import logging
import time
import math
import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from db.connection import fetch_df

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Database"])

class DBQueryRequest(BaseModel):
    sql: str

@router.post("/db/query")
def execute_sql_query(req: DBQueryRequest):
    """ 执行自定义 SQL 查询 (限 Admin) """
    try:
        sql_upper = req.sql.strip().upper()
        
        if not sql_upper.startswith("SELECT"):
            raise HTTPException(status_code=400, detail="仅支持 SELECT 查询")
        
        dangerous_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE", "EXEC", "EXECUTE"]
        for keyword in dangerous_keywords:
            if f" {keyword} " in f" {sql_upper} ":
                raise HTTPException(status_code=400, detail=f"禁止执行包含 {keyword} 的查询")
        
        if "LIMIT" not in sql_upper:
            if sql_upper.rstrip().endswith(";"):
                req.sql = req.sql.rstrip()[:-1] + " LIMIT 1000;"
            else:
                req.sql = req.sql.rstrip() + " LIMIT 1000"
        
        start_time = time.time()
        timeout_seconds = 30
        
        df = fetch_df(req.sql)
        
        if time.time() - start_time > timeout_seconds:
            logger.warning(f"SQL 查询超时: {req.sql[:100]}...")
        
        if len(df) > 10000:
            logger.warning(f"SQL 查询返回行数过多: {len(df)}，已截断到 10000 行")
            df = df.head(10000)
        
        df = df.replace([np.inf, -np.inf], np.nan)
        df_obj = df.astype(object).where(pd.notnull(df), None)
        return {
            "status": "success",
            "data": df_obj.to_dict('records'),
            "columns": df.columns.tolist(),
            "row_count": len(df_obj)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"SQL 执行失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))