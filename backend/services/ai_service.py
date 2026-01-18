"""
AI服务层 - 支持多模型（DeepSeek、Qwen等）
"""
import requests
import json
import logging
from typing import Optional, Dict, List
from db.ai_config_repository import AIConfigRepository
from db.ai_model_config_repository import AIModelConfigRepository

logger = logging.getLogger(__name__)

class AIService:
    """AI服务 - 支持多模型"""
    
    
    @staticmethod
    def get_default_model() -> Optional[Dict]:
        """获取默认模型配置（第一个启用的模型）"""
        models = AIModelConfigRepository.get_active_models()
        if models:
            return models[0]
        return None
    
    @staticmethod
    def get_prompt(prompt_type: str) -> str:
        """获取Prompt模板"""
        default_prompts = {
            "recommend": """## 【角色设定】
你是一位拥有20年实战经验的A股"机构派游资"。你既看重公募基金的安全垫（业绩、壁垒、无雷），又擅长捕捉顶级游资的技术买点（洗盘结束、强力反转）。

## 【输入信息】
当前日期：{date}
核心热点/主线：{hot_sectors}

## 【任务目标】
基于上述市场环境，挖掘 3到5只 符合"绩优白马 + 底部旭日东升（强力反转）"特征的潜力个股。

## 【严格筛选标准】

### 1. 硬核风控（一票否决制）
基本面底线：必须是细分行业龙头、隐形冠军或绩优股。要有真实的业绩支撑或不可替代的技术壁垒。
排雷行动：严禁推荐业绩连年亏损、有违规减持历史、大额商誉减值风险或有财务造假嫌疑的标的。

### 2. 技术形态（特定反转逻辑）
洗盘阶段（缩量缓跌）：个股前期经历了一段明显的"缩量缓跌"（阴跌）调整，换手率降低，表明浮筹清洗充分，空头力量衰竭。
启动信号（放量大阳）：近期（1-3日内）突然走出一根"放量大阳线"（涨幅>5%）。
强度确认（光头阳线）：该阳线收盘价必须接近全天最高价（无明显上影线，实体饱满），且收盘站稳，确立主力做多意志坚决，非试盘行为。

### 3. 股性特征
优先选择 科创板(688) 或 创业板(300)，具备 20CM 高弹性基因，历史股性活跃。

## 【输出格式】
请严格按以下结构输出 3只 标的分析报告：

### [肥羊名称 & 代码]

1. 基本面逻辑（机构安全垫）：
用简练语言概括其核心业绩增长点或行业地位（为什么跌下来机构敢接？）。

2. 技术面解析（游资爆发力）：
洗盘特征：描述此前缩量调整的周期和幅度。
反转信号：分析这根"启动大阳线"的量能倍数（如：量比>3）及K线形态（是否光头、是否反包）。

3. 实战策略：
低吸位：[依托大阳线实体的支撑价格，如：阳线实体中下部]
防守位（止损）：[跌破启动阳线的最低价]
目标位：[上方筹码密集区或重要均线压力位]""",
            
            "analyze": """你是一位拥有20年实战经验的A股"机构派游资"。你既看重公募基金的安全垫（业绩、壁垒、无雷），又擅长捕捉顶级游资的技术买点（洗盘结束、强力反转）。

分析日期：{date}
标的名称：{sheep_name}
所属板块：{sectors}

请你分析以下数据，按照以下框架输出分析报告：
1.  趋势定性：当前处于趋势的哪个阶段（萌芽/启动/加速/滞涨/反转）？用1-2个核心指标（如均线排列、MACD形态、量能趋势）佐证。
2.  资金验证：近5个交易日的主力资金流向（净流入/流出）、龙虎榜机构/游资席位动向（如有），判断资金是否具备持续性。
3.  支撑压力：当前股价的关键支撑位（强支撑/弱支撑）、压力位（强压力/弱压力），说明判断依据。
4.  板块联动：是否属于某个热门板块？所属热门板块的趋势强度如何？该股在板块中是龙头/跟风/补涨？板块轮动周期是否匹配个股趋势。
5.  操作建议：趋势型游资视角下，适合持仓/加仓/减仓/清仓的信号是什么？明确触发条件。
6.  风险预警：哪些信号出现意味着趋势可能终结（如量价背离、均线破位、板块退潮）？

数据：
{data}

要求：结论先行，语言简洁，数据支撑，拒绝空话套话。"""
        }
        
        prompt = AIConfigRepository.get_config(f"prompt_{prompt_type}")
        if not prompt:
            prompt = default_prompts.get(prompt_type, "")
        return prompt
    
    @classmethod
    def call_ai_api(cls, user_id: int, model_name: Optional[str] = None, messages: List[Dict] = None, 
                    temperature: float = 0.7, max_tokens: int = 2000) -> Optional[str]:
        """调用AI API（支持多模型）"""
        # 获取模型配置
        if model_name:
            model_config = AIModelConfigRepository.get_model_by_name(model_name)
        else:
            model_config = cls.get_default_model()
        
        if not model_config:
            raise ValueError("没有可用的AI模型配置，请联系管理员")
        
        # 使用模型配置的API Key
        api_key = model_config.get("api_key")
        
        if not api_key:
            raise ValueError("API Key未配置，请在AI管理设置中配置模型API Key")
        
        api_url = model_config["api_url"]
        model = model_config["model_name"]
        
        try:
            headers = {
                "Content-Type": "application/json",
            }
            
            # 根据模型类型设置不同的认证方式
            if "deepseek" in model.lower():
                headers["Authorization"] = f"Bearer {api_key}"
            elif "qwen" in model.lower() or "dashscope" in api_url.lower():
                headers["Authorization"] = f"Bearer {api_key}"
            else:
                headers["Authorization"] = f"Bearer {api_key}"
            
            # 根据模型类型构建不同的请求体
            if "qwen" in model.lower() or "dashscope" in api_url.lower():
                # 通义千问API格式（DashScope）
                data = {
                    "model": model,
                    "input": {
                        "messages": messages
                    },
                    "parameters": {
                        "temperature": temperature,
                        "max_tokens": max_tokens
                    }
                }
            else:
                # DeepSeek等标准OpenAI格式
                data = {
                    "model": model,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                    "stream": False
                }
            
            response = requests.post(
                api_url,
                headers=headers,
                json=data,
                timeout=120  # 120秒超时
            )
            
            if response.status_code != 200:
                error_msg = f"AI API调用失败: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            result = response.json()
            
            # 解析不同格式的响应
            if "qwen" in model.lower() or "dashscope" in api_url.lower():
                # 通义千问响应格式（DashScope）
                if "output" in result:
                    if "choices" in result["output"] and len(result["output"]["choices"]) > 0:
                        return result["output"]["choices"][0]["message"]["content"]
                    elif "text" in result["output"]:
                        return result["output"]["text"]
                # 兼容其他可能的响应格式
                if "choices" in result and len(result["choices"]) > 0:
                    if "message" in result["choices"][0]:
                        return result["choices"][0]["message"]["content"]
                    elif "text" in result["choices"][0]:
                        return result["choices"][0]["text"]
            else:
                # 标准OpenAI格式
                if "choices" in result and len(result["choices"]) > 0:
                    return result["choices"][0]["message"]["content"]
            
            raise Exception(f"AI API返回异常: {result}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"AI API请求异常: {e}")
            raise Exception(f"API请求失败: {str(e)}")
        except Exception as e:
            logger.error(f"AI API调用异常: {e}")
            raise
    
    @classmethod
    def recommend_sheep(cls, user_id: int, hot_sheep: List[Dict], sectors: List[Dict], model_name: Optional[str] = None, custom_prompt: Optional[str] = None) -> str:
        """AI推荐肥羊"""
        try:
            # 准备数据
            sheep_data = []
            for sheep in hot_sheep[:20]:  # 取前20只
                sheep_data.append({
                    "代码": sheep.get("sheep_code"),
                    "名称": sheep.get("sheep_name"),
                    "排名": sheep.get("rank"),
                    "涨幅": sheep.get("change_pct"),
                    "成交量": sheep.get("volume"),
                    "来源": sheep.get("source")
                })
            
            sectors_data = []
            for sector in sectors[:10]:  # 取前10个板块
                sectors_data.append({
                    "板块": sector.get("sector_name"),
                    "热门股数量": sector.get("hot_count", 0),
                    "热度分数": sector.get("hot_score", 0)
                })
            
            data_str = f"""
热门肥羊列表：
{json.dumps(sheep_data, ensure_ascii=False, indent=2)}

热门板块列表：
{json.dumps(sectors_data, ensure_ascii=False, indent=2)}
"""
            
            # 获取prompt并填充变量
            from datetime import datetime
            
            # 如果提供了自定义提示词，直接使用；否则从配置获取
            if custom_prompt:
                prompt_template = custom_prompt
            else:
                prompt_template = cls.get_prompt("recommend")
            
            # 格式化热门板块列表
            sectors_list = []
            for sector in sectors[:10]:
                sectors_list.append(f"- {sector.get('sector_name')}（热门股数量：{sector.get('hot_count', 0)}，热度分数：{sector.get('hot_score', 0)}）")
            hot_sectors_str = '\n'.join(sectors_list) if sectors_list else '暂无热门板块'
            
            # 填充变量（如果自定义提示词中没有变量，format不会报错）
            try:
                prompt = prompt_template.format(
                    date=datetime.now().strftime('%Y-%m-%d'),
                    hot_sectors=hot_sectors_str,
                    data=data_str
                )
            except KeyError:
                # 如果自定义提示词中没有这些变量，直接使用
                prompt = prompt_template
            
            messages = [
                {"role": "system", "content": "你是老K，一位拥有20年实战经验的A股机构派游资。你既看重公募基金的安全垫（业绩、壁垒、无雷），又擅长捕捉顶级游资的技术买点（洗盘结束、强力反转）"},
                {"role": "user", "content": prompt}
            ]
            
            result = cls.call_ai_api(user_id, model_name, messages, temperature=0.7, max_tokens=2000)
            return result
            
        except Exception as e:
            logger.error(f"AI推荐肥羊失败: {e}")
            raise
    
    @classmethod
    def analyze_sheep(cls, user_id: int, sheep_code: str, sheep_name: str, sheep_data: Dict, model_name: Optional[str] = None, custom_prompt: Optional[str] = None) -> str:
        """AI分析肥羊"""
        try:
            # 准备数据
            kline = sheep_data.get('kline', [])
            money_flow = sheep_data.get('money_flow', [])
            
            # 格式化K线数据（只显示最近10天的关键信息）
            kline_summary = []
            if kline and isinstance(kline, list):
                for item in kline[-10:]:  # 最近10天
                    kline_summary.append({
                        "日期": item.get('trade_date', 'N/A'),
                        "收盘价": item.get('close_price', 'N/A'),
                        "涨跌": f"{item.get('close_price', 0) - item.get('open_price', 0):.2f}" if item.get('close_price') and item.get('open_price') else 'N/A',
                        "成交量": item.get('volume', 'N/A'),
                        "MA5": item.get('ma5', 'N/A'),
                        "MA20": item.get('ma20', 'N/A'),
                    })
            
            # 格式化资金流向数据（汇总最近10天）
            money_flow_summary = {}
            if money_flow and isinstance(money_flow, list):
                recent_flows = money_flow[-10:]  # 最近10天
                if recent_flows:
                    money_flow_summary = {
                        "最近10天主力净流入": sum(item.get('main_net_inflow', 0) for item in recent_flows),
                        "最近10天超大单流入": sum(item.get('super_large_inflow', 0) for item in recent_flows),
                        "最近10天大单流入": sum(item.get('large_inflow', 0) for item in recent_flows),
                    }
            
            data_str = f"""
当前价格：{sheep_data.get('current_price', 'N/A')}
涨跌幅：{sheep_data.get('change_pct', 'N/A')}%
成交量：{sheep_data.get('volume', 'N/A')}
K线数据（最近10天）：{json.dumps(kline_summary, ensure_ascii=False) if kline_summary else 'N/A'}
资金流向（最近10天汇总）：{json.dumps(money_flow_summary, ensure_ascii=False) if money_flow_summary else 'N/A'}
"""
            
            # 获取prompt并填充变量
            from datetime import datetime
            
            # 如果提供了自定义提示词，直接使用；否则从配置获取
            if custom_prompt:
                prompt_template = custom_prompt
            else:
                prompt_template = cls.get_prompt("analyze")
            
            # 获取板块信息
            sectors_str = ', '.join(sheep_data.get('sectors', [])) if sheep_data.get('sectors') else 'N/A'
            
            # 填充变量（如果自定义提示词中没有变量，format不会报错）
            try:
                prompt = prompt_template.format(
                    date=datetime.now().strftime('%Y-%m-%d'),
                    sheep_name=sheep_name,
                    sectors=sectors_str,
                    data=data_str
                )
            except KeyError:
                # 如果自定义提示词中没有这些变量，直接使用
                prompt = prompt_template
            
            messages = [
                {"role": "system", "content": "你是一位资深的肥羊投资分析师，擅长技术分析和基本面分析。"},
                {"role": "user", "content": prompt}
            ]
            
            result = cls.call_ai_api(user_id, model_name, messages, temperature=0.7, max_tokens=2000)
            return result
            
        except Exception as e:
            logger.error(f"AI分析肥羊失败: {e}")
            raise
