# /backend/api/routes/ai.py

import logging
import json
import math
import httpx
import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Optional
from db.connection import get_db_connection, fetch_df
from .users import get_current_user_id

logger = logging.getLogger(__name__)
router = APIRouter(tags=["AI"])

class UserAIConfig(BaseModel):
    model_provider: str = "openai"
    model_name: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    system_prompt: Optional[str] = None
    max_tokens: int = 4096
    temperature: float = 0.7

class UserAIConfigUpdate(BaseModel):
    model_provider: Optional[str] = None
    model_name: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    system_prompt: Optional[str] = None
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None

class PromptTemplateCreate(BaseModel):
    name: str
    content: str
    is_default: bool = False

class PromptTemplateUpdate(BaseModel):
    name: Optional[str] = None
    content: Optional[str] = None
    is_default: Optional[bool] = None

class AIAnalyzeRequest(BaseModel):
    ts_code: str
    template_id: Optional[int] = None
    force_refresh: Optional[bool] = False

class SelectTemplateRequest(BaseModel):
    template_id: Optional[int] = None

@router.get("/users/me/ai-config")
async def get_my_ai_config(request: Request):
    """获取当前用户的AI配置"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            row = con.execute(
                "SELECT model_provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature FROM user_ai_config WHERE user_id = ?",
                (user_id,)
            ).fetchone()
        if row:
            return {
                "model_provider": row[0],
                "model_name": row[1],
                "api_key": row[2],
                "base_url": row[3],
                "system_prompt": row[4],
                "max_tokens": row[5],
                "temperature": row[6]
            }
        return {
            "model_provider": "openai",
            "model_name": None,
            "api_key": None,
            "base_url": None,
            "system_prompt": None,
            "max_tokens": 4096,
            "temperature": 0.7
        }
    except Exception as e:
        logger.error(f"获取AI配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/me/ai-config")
async def update_my_ai_config(request: Request, config: UserAIConfigUpdate):
    """更新当前用户的AI配置"""
    user_id = await get_current_user_id(request)
    try:
        updates = []
        params = []
        
        if config.model_provider is not None:
            updates.append("model_provider = ?")
            params.append(config.model_provider)
        if config.model_name is not None:
            updates.append("model_name = ?")
            params.append(config.model_name)
        if config.api_key is not None:
            updates.append("api_key = ?")
            params.append(config.api_key)
        if config.base_url is not None:
            updates.append("base_url = ?")
            params.append(config.base_url)
        if config.system_prompt is not None:
            updates.append("system_prompt = ?")
            params.append(config.system_prompt)
        if config.max_tokens is not None:
            updates.append("max_tokens = ?")
            params.append(config.max_tokens)
        if config.temperature is not None:
            updates.append("temperature = ?")
            params.append(config.temperature)
        
        if not updates:
            return {"message": "没有需要更新的字段"}
        
        updates.append("updated_at = CURRENT_TIMESTAMP")
        
        with get_db_connection() as con:
            exists = con.execute("SELECT 1 FROM user_ai_config WHERE user_id = ?", (user_id,)).fetchone()
            if exists:
                sql = f"UPDATE user_ai_config SET {', '.join(updates)} WHERE user_id = ?"
                params.append(user_id)
                con.execute(sql, params)
            else:
                cols = [u.split(' = ')[0] for u in updates if 'CURRENT_TIMESTAMP' not in u]
                vals_placeholders = ', '.join(['?' for _ in cols])
                insert_sql = f"INSERT INTO user_ai_config (user_id, {', '.join(cols)}) VALUES (?, {vals_placeholders})"
                insert_params = [user_id] + params
                con.execute(insert_sql, insert_params)
        
        return {"message": "AI配置已更新"}
    except Exception as e:
        logger.error(f"更新AI配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/users/me/prompt-templates")
async def get_prompt_templates(request: Request):
    """获取当前用户的提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            rows = con.execute(
                "SELECT id, name, content, is_default, created_at, updated_at FROM user_prompt_templates WHERE user_id = ? ORDER BY is_default DESC, created_at DESC",
                (user_id,)
            ).fetchall()
        return [{"id": r[0], "name": r[1], "content": r[2], "is_default": r[3], "created_at": r[4], "updated_at": r[5]} for r in rows]
    except Exception as e:
        logger.error(f"获取提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/users/me/prompt-templates")
async def create_prompt_template(request: Request, template: PromptTemplateCreate):
    """创建提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            if template.is_default:
                con.execute("UPDATE user_prompt_templates SET is_default = FALSE WHERE user_id = ?", (user_id,))
            max_id = con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM user_prompt_templates").fetchone()[0]
            con.execute(
                "INSERT INTO user_prompt_templates (id, user_id, name, content, is_default) VALUES (?, ?, ?, ?, ?)",
                (max_id, user_id, template.name, template.content, template.is_default)
            )
        return {"message": "模板创建成功"}
    except Exception as e:
        logger.error(f"创建提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/me/prompt-templates/{template_id}")
async def update_prompt_template(request: Request, template_id: int, template: PromptTemplateUpdate):
    """更新提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            exists = con.execute("SELECT 1 FROM user_prompt_templates WHERE id = ? AND user_id = ?", (template_id, user_id)).fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail="模板不存在")
            
            if template.is_default:
                con.execute("UPDATE user_prompt_templates SET is_default = FALSE WHERE user_id = ?", (user_id,))
            
            updates = []
            params = []
            if template.name is not None:
                updates.append("name = ?")
                params.append(template.name)
            if template.content is not None:
                updates.append("content = ?")
                params.append(template.content)
            if template.is_default is not None:
                updates.append("is_default = ?")
                params.append(template.is_default)
            
            if updates:
                updates.append("updated_at = CURRENT_TIMESTAMP")
                params.append(template_id)
                con.execute(f"UPDATE user_prompt_templates SET {', '.join(updates)} WHERE id = ?", params)
        return {"message": "模板更新成功"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/users/me/prompt-templates/{template_id}")
async def delete_prompt_template(request: Request, template_id: int):
    """删除提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            con.execute("DELETE FROM user_prompt_templates WHERE id = ? AND user_id = ?", (template_id, user_id))
        return {"message": "模板已删除"}
    except Exception as e:
        logger.error(f"删除提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/users/me/selected-template")
async def get_selected_template(request: Request):
    """获取当前选中的模板ID"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            row = con.execute(
                "SELECT selected_template_id FROM user_ai_config WHERE user_id = ?",
                (user_id,)
            ).fetchone()
        return {"selected_template_id": row[0] if row else None}
    except Exception as e:
        logger.error(f"获取选中模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/me/selected-template")
async def select_template(request: Request, body: SelectTemplateRequest):
    """设置选中的模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            exists = con.execute("SELECT 1 FROM user_ai_config WHERE user_id = ?", (user_id,)).fetchone()
            if exists:
                con.execute("UPDATE user_ai_config SET selected_template_id = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?", 
                    (body.template_id, user_id))
            else:
                con.execute("INSERT INTO user_ai_config (user_id, selected_template_id) VALUES (?, ?)", 
                    (user_id, body.template_id))
        return {"message": "模板已选中"}
    except Exception as e:
        logger.error(f"选择模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stock/analyze")
async def analyze_stock_with_ai(request: Request, body: AIAnalyzeRequest):
    """使用AI分析股票"""
    user_id = await get_current_user_id(request)
    try:
        # 获取最新交易日
        with get_db_connection() as con:
            latest_trade = con.execute(
                "SELECT MAX(trade_date) FROM daily_price WHERE ts_code = ?",
                (body.ts_code,)
            ).fetchone()
        
        if not latest_trade or not latest_trade[0]:
            raise HTTPException(status_code=400, detail=f"未找到股票 {body.ts_code} 的行情数据")
        
        latest_trade_date = str(latest_trade[0])
        
        # 检查缓存（如果不是强制刷新）
        if not body.force_refresh:
            with get_db_connection() as con:
                cache = con.execute(
                    "SELECT analysis_result, created_at FROM ai_analysis_cache WHERE user_id = ? AND ts_code = ? AND trade_date = ? ORDER BY created_at DESC LIMIT 1",
                    (user_id, body.ts_code, latest_trade_date)
                ).fetchone()
            
            if cache:
                logger.info(f"返回缓存的分析结果: {body.ts_code} {latest_trade_date}")
                return {
                    "analysis": cache[0],
                    "ts_code": body.ts_code,
                    "trade_date": latest_trade_date,
                    "from_cache": True
                }
        
        # 获取用户AI配置
        with get_db_connection() as con:
            config = con.execute(
                "SELECT model_provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature FROM user_ai_config WHERE user_id = ?",
                (user_id,)
            ).fetchone()
        
        if not config or not config[2]:
            raise HTTPException(status_code=400, detail="请先在设置中配置API Key")
        
        model_provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature = config
        
        # 获取模板
        template_id = body.template_id
        if not template_id:
            with get_db_connection() as con:
                tpl = con.execute(
                    "SELECT content FROM user_prompt_templates WHERE user_id = ? AND is_default = TRUE",
                    (user_id,)
                ).fetchone()
                template_content = tpl[0] if tpl else None
        else:
            with get_db_connection() as con:
                tpl = con.execute(
                    "SELECT content FROM user_prompt_templates WHERE id = ? AND user_id = ?",
                    (template_id, user_id)
                ).fetchone()
                template_content = tpl[0] if tpl else None
        
        # 获取股票基本信息
        stock_basic = None
        with get_db_connection() as con:
            basic = con.execute(
                "SELECT ts_code, name, industry, market FROM stock_basic WHERE ts_code = ?",
                (body.ts_code,)
            ).fetchone()
            if basic:
                stock_basic = {"ts_code": basic[0], "name": basic[1], "industry": basic[2], "market": basic[3]}
        
        # 获取30日行情数据 - 简化格式
        with get_db_connection() as con:
            prices = con.execute("""
                SELECT trade_date, close, vol, pct_chg 
                FROM daily_price WHERE ts_code = ? ORDER BY trade_date DESC LIMIT 30
            """, (body.ts_code,)).fetchall()
        
        # 格式化为紧凑的表格形式: 日期,收盘价,成交量(万手),涨跌幅
        price_lines = []
        for p in reversed(prices):
            date_str = str(p[0])[:10]  # 只取日期部分
            close = round(float(p[1]), 2) if p[1] else 0
            vol_wan = round(float(p[2]) / 10000, 2) if p[2] else 0  # 转换为万手
            pct_chg = round(float(p[3]), 2) if p[3] else 0
            price_lines.append(f"{date_str},{close},{vol_wan},{pct_chg}")
        price_data = "\n".join(price_lines)
        
        logger.debug(f"price_data rows: {len(price_lines)}")
        
        # 获取资金流向 - 简化为只保留主力净流入(万元)
        money_flow = None
        with get_db_connection() as con:
            mf = con.execute("""
                SELECT trade_date, net_mf_amount, net_mf_ratio
                FROM stock_moneyflow WHERE ts_code = ? ORDER BY trade_date DESC LIMIT 10
            """, (body.ts_code,)).fetchall()
        
        if mf:
            mf_lines = []
            for m in reversed(mf):
                date_str = str(m[0])[:10]
                net_amt = round(float(m[1]) / 10000, 2) if m[1] else 0  # 转换为万元
                net_ratio = round(float(m[2]), 2) if m[2] else 0
                mf_lines.append(f"{date_str},{net_amt},{net_ratio}")
            money_flow = "\n".join(mf_lines)
        
        # 获取融资融券数据 - 简化为只保留融资余额(亿)
        margin_data = None
        with get_db_connection() as con:
            mg = con.execute("""
                SELECT trade_date, rzye
                FROM stock_margin WHERE ts_code = ? ORDER BY trade_date DESC LIMIT 10
            """, (body.ts_code,)).fetchall()
        
        if mg:
            mg_lines = []
            for m in reversed(mg):
                date_str = str(m[0])[:10]
                rzye_yi = round(float(m[1]) / 100000000, 2) if m[1] else 0  # 转换为亿
                mg_lines.append(f"{date_str},{rzye_yi}")
            margin_data = "\n".join(mg_lines)
        
        # 获取持仓信息
        holding = None
        with get_db_connection() as con:
            h = con.execute(
                "SELECT shares, avg_cost FROM user_holdings WHERE user_id = ? AND ts_code = ?",
                (user_id, body.ts_code)
            ).fetchone()
            if h:
                shares = int(float(h[0]))
                avg_cost = round(float(h[1]), 2) if h[1] else 0
                holding = f"持仓{shares}股，成本价{avg_cost}元"
        # 判断是否在开盘时间段
        from etl.calendar import trading_calendar
        is_trading_time = trading_calendar.is_trading_time()
        
        # 获取实时行情数据（如果是开盘时间段）
        realtime_data = None
        if is_trading_time:
            try:
                from etl.sync import sync_engine
                rt_df = sync_engine.provider.realtime_quote(ts_code=body.ts_code, src="sina")
                if rt_df is not None and not rt_df.empty:
                    rt = rt_df.iloc[0]
                    current_price = rt.get('price', 0)
                    current_vol = rt.get('vol', 0)
                    current_amount = rt.get('amount', 0)
                    current_pct_chg = rt.get('pct_chg', 0)
                    realtime_data = f"最新价:{current_price} | 成交量:{round(float(current_vol)/10000,2)}万手 | 成交额:{round(float(current_amount)/10000,2)}万 | 涨跌幅:{current_pct_chg}%"
                    logger.info(f"获取实时行情成功: {body.ts_code} price={current_price}")
            except Exception as e:
                logger.warning(f"获取实时行情失败: {e}")
        
        # 获取关联个股（同行业上市公司）
        related_stocks = []
        if stock_basic and stock_basic.get("industry"):
            with get_db_connection() as con:
                related = con.execute("""
                    SELECT ts_code, name FROM stock_basic 
                    WHERE industry = ? AND ts_code != ? AND list_status = 'L'
                    ORDER BY list_date DESC LIMIT 5
                """, (stock_basic["industry"], body.ts_code)).fetchall()
                if related:
                    related_stocks = [{"ts_code": r[0], "name": r[1]} for r in related]
        
        # 格式化数据（用于日志）
        stock_name = stock_basic.get("name", "") if stock_basic else ""
        ts_code = stock_basic.get("ts_code", body.ts_code) if stock_basic else body.ts_code
        industry = stock_basic.get("industry", "") if stock_basic else ""
        market = stock_basic.get("market", "") if stock_basic else ""
        
        # 构建实时行情和关联个股区块
        realtime_section = ""
        if is_trading_time and realtime_data:
            realtime_section = f"""
## ⏰ 盘中实时数据(标记：当前为开盘时间段)
{realtime_data}
"""
        
        related_section = ""
        if related_stocks:
            related_list = "、".join([f"{s['name']}({s['ts_code']})" for s in related_stocks])
            related_section = f"""
## 关联个股(同行业)
{related_list}

请在分析中重点关注上述关联个股的表现和联动性，特别关注有技术壁垒且业绩预期向上的标的。"""
        
        # 构建提示词
        if template_content:
            # 使用用户模板，替换占位符
            prompt = template_content
            prompt = prompt.replace("{stock_name}", stock_name)
            prompt = prompt.replace("{ts_code}", ts_code)
            prompt = prompt.replace("{industry}", industry)
            prompt = prompt.replace("{market}", market)
            prompt = prompt.replace("{realtime_section}", realtime_section)
            prompt = prompt.replace("{price_data}", price_data or "暂无")
            prompt = prompt.replace("{money_flow}", money_flow or "暂无")
            prompt = prompt.replace("{margin_data}", margin_data or "暂无")
            prompt = prompt.replace("{holding}", holding or "暂无持仓")
            prompt = prompt.replace("{related_section}", related_section)
        else:
            prompt = f"""你是A股量化分析师，请对以下股票进行全面深入的分析。

## {stock_name}({ts_code})
行业：{industry} | 市场：{market}
{realtime_section}
## 近30日行情(日期,收盘价,成交量万手,涨跌幅%)
{price_data}

## 主力净流入近10日(日期,净额万元,占比%)
{money_flow}

## 融资余额近10日(日期,余额亿元)
{margin_data}

## 持仓
{holding}
{related_section}
---
请给出详细分析：
1. **技术面分析**：趋势方向、K线形态、关键支撑压力位、量价配合、均线系统
2. **资金面分析**：主力资金动向、融资余额变化趋势、筹码分布
3. **关联个股分析**：同行业其他标的的表现对比，有技术壁垒且业绩预期向上的公司
4. **操作建议**：明确买/卖/观望，给出具体价位和仓位建议
5. **持仓建议**：如有持仓，给出止盈止损位和加减仓策略
6. **风险提示**：主要风险因素

要求：结论先行，分析全面深入。"""
        
        # 记录数据状态以便调试
        logger.info(f"AI分析数据准备: stock_basic={'有' if stock_basic else '无'}, "
                    f"price_data行数={len(price_data.split(chr(10))) if price_data else 0}, "
                    f"money_flow行数={len(money_flow.split(chr(10))) if money_flow else 0}, "
                    f"margin_data行数={len(margin_data.split(chr(10))) if margin_data else 0}, "
                    f"holding={'有' if holding else '无'}, "
                    f"is_trading_time={is_trading_time}, "
                    f"realtime_data={'有' if realtime_data else '无'}, "
                    f"related_stocks数={len(related_stocks)}")
        
        # 记录提示词长度和部分内容
        logger.info(f"AI分析提示词长度: {len(prompt)}")
        if len(prompt) > 1000:
            logger.info(f"AI分析提示词前1000字符: {prompt[:1000]}")
            logger.debug(f"AI分析提示词完整内容: {prompt}")
        else:
            logger.info(f"AI分析提示词: {prompt}")
        
        # 调用AI
        model = model_name or "deepseek-chat"
        logger.info(f"AI provider: {model_provider}, model: {model}")
        if model_provider == "deepseek":
            if not base_url:
                base_url = "https://api.deepseek.com/v1"
            base_url = base_url.rstrip('/')
            url = f"{base_url}/chat/completions"
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt or "你是一个专业的A股交易分析师。"},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": max_tokens or 4096,
                "temperature": temperature or 0.7
            }
        else:  # openai
            if not base_url:
                base_url = "https://api.openai.com/v1"
            base_url = base_url.rstrip('/')
            url = f"{base_url}/chat/completions"
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            payload = {
                "model": model or "gpt-4",
                "messages": [
                    {"role": "system", "content": system_prompt or "你是一个专业的A股交易分析师。"},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": max_tokens or 4096,
                "temperature": temperature or 0.7
            }
        
        logger.info(f"AI分析请求: {body.ts_code}, 模型: {model}, 交易日: {latest_trade_date}")
        
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                error_detail = resp.text
                logger.error(f"AI API error: {error_detail}")
                raise HTTPException(status_code=502, detail=f"AI服务调用失败: {error_detail}")
            result = resp.json()
            analysis = result["choices"][0]["message"]["content"]
        
        # 保存到缓存
        with get_db_connection() as con:
            # 先删除旧缓存
            con.execute(
                "DELETE FROM ai_analysis_cache WHERE user_id = ? AND ts_code = ? AND trade_date = ?",
                (user_id, body.ts_code, latest_trade_date)
            )
            # 生成新id
            max_id = con.execute("SELECT COALESCE(MAX(id), 0) FROM ai_analysis_cache").fetchone()[0]
            new_id = max_id + 1
            # 插入新缓存
            con.execute(
                "INSERT INTO ai_analysis_cache (id, user_id, ts_code, trade_date, analysis_result, model_name) VALUES (?, ?, ?, ?, ?, ?)",
                (new_id, user_id, body.ts_code, latest_trade_date, analysis, model)
            )
        
        logger.info(f"AI分析完成并缓存: {body.ts_code} {latest_trade_date}")
        
        return {
            "analysis": analysis,
            "ts_code": body.ts_code,
            "trade_date": latest_trade_date,
            "from_cache": False
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        logger.error(f"AI分析失败: {e}")
        logger.error(f"AI分析失败详情: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))