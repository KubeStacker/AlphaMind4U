"""
AI服务层 - 集成DeepSeek API
"""
import requests
import json
import logging
from typing import Optional, Dict, List
from db.ai_config_repository import AIConfigRepository

logger = logging.getLogger(__name__)

class AIService:
    """AI服务 - DeepSeek API集成"""
    
    DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
    
    @staticmethod
    def get_api_key() -> Optional[str]:
        """获取DeepSeek API Key"""
        return AIConfigRepository.get_config("api_key")
    
    @staticmethod
    def get_prompt(prompt_type: str) -> str:
        """获取Prompt模板"""
        default_prompts = {
            "recommend": """你是一位资深的股票投资分析师。请根据以下热门股票数据，分析并推荐最有投资价值的股票。

热门股票数据：
{data}

请从以下角度进行分析：
1. 行业趋势和板块热度
2. 技术面分析（价格走势、成交量等）
3. 资金流向
4. 市场情绪和热度持续性

请给出3-5只最值得关注的股票推荐，并说明推荐理由。格式要求：
- 股票代码：股票名称
- 推荐理由：（简要说明）
- 风险提示：（如有）""",
            
            "analyze": """你是一位资深的股票投资分析师。请对以下股票进行深度分析。

股票信息：
{data}

请从以下角度进行专业分析：
1. 基本面分析（行业地位、财务状况等）
2. 技术面分析（K线形态、均线系统、成交量等）
3. 资金流向分析
4. 市场情绪和热度分析
5. 风险评估

请给出专业的投资建议和操作建议。"""
        }
        
        prompt = AIConfigRepository.get_config(f"prompt_{prompt_type}")
        if not prompt:
            prompt = default_prompts.get(prompt_type, "")
        return prompt
    
    @classmethod
    def call_deepseek_api(cls, messages: List[Dict], temperature: float = 0.7, max_tokens: int = 2000) -> Optional[str]:
        """调用DeepSeek API"""
        api_key = cls.get_api_key()
        if not api_key:
            raise ValueError("DeepSeek API Key未配置，请在设置中配置API Key")
        
        try:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            
            data = {
                "model": "deepseek-chat",
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "stream": False
            }
            
            response = requests.post(
                cls.DEEPSEEK_API_URL,
                headers=headers,
                json=data,
                timeout=120  # 120秒超时，AI分析可能需要较长时间
            )
            
            if response.status_code != 200:
                error_msg = f"DeepSeek API调用失败: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            result = response.json()
            if "choices" in result and len(result["choices"]) > 0:
                return result["choices"][0]["message"]["content"]
            else:
                raise Exception(f"DeepSeek API返回异常: {result}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"DeepSeek API请求异常: {e}")
            raise Exception(f"API请求失败: {str(e)}")
        except Exception as e:
            logger.error(f"DeepSeek API调用异常: {e}")
            raise
    
    @classmethod
    def recommend_stocks(cls, hot_stocks: List[Dict], sectors: List[Dict]) -> str:
        """AI推荐股票"""
        try:
            # 准备数据
            stocks_data = []
            for stock in hot_stocks[:20]:  # 取前20只
                stocks_data.append({
                    "代码": stock.get("stock_code"),
                    "名称": stock.get("stock_name"),
                    "排名": stock.get("rank"),
                    "涨幅": stock.get("change_pct"),
                    "成交量": stock.get("volume"),
                    "来源": stock.get("source")
                })
            
            sectors_data = []
            for sector in sectors[:10]:  # 取前10个板块
                sectors_data.append({
                    "板块": sector.get("sector_name"),
                    "热门股数量": sector.get("hot_count", 0),
                    "热度分数": sector.get("hot_score", 0)
                })
            
            data_str = f"""
热门股票列表：
{json.dumps(stocks_data, ensure_ascii=False, indent=2)}

热门板块列表：
{json.dumps(sectors_data, ensure_ascii=False, indent=2)}
"""
            
            # 获取prompt
            prompt_template = cls.get_prompt("recommend")
            prompt = prompt_template.format(data=data_str)
            
            messages = [
                {"role": "system", "content": "你是一位资深的股票投资分析师，擅长分析市场趋势和股票投资价值。"},
                {"role": "user", "content": prompt}
            ]
            
            result = cls.call_deepseek_api(messages, temperature=0.7, max_tokens=2000)
            return result
            
        except Exception as e:
            logger.error(f"AI推荐股票失败: {e}")
            raise
    
    @classmethod
    def analyze_stock(cls, stock_code: str, stock_name: str, stock_data: Dict) -> str:
        """AI分析股票"""
        try:
            # 准备数据
            kline = stock_data.get('kline', [])
            money_flow = stock_data.get('money_flow', [])
            
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
股票代码：{stock_code}
股票名称：{stock_name}
当前价格：{stock_data.get('current_price', 'N/A')}
涨跌幅：{stock_data.get('change_pct', 'N/A')}%
成交量：{stock_data.get('volume', 'N/A')}
所属板块：{', '.join(stock_data.get('sectors', [])) if stock_data.get('sectors') else 'N/A'}
K线数据（最近10天）：{json.dumps(kline_summary, ensure_ascii=False) if kline_summary else 'N/A'}
资金流向（最近10天汇总）：{json.dumps(money_flow_summary, ensure_ascii=False) if money_flow_summary else 'N/A'}
"""
            
            # 获取prompt
            prompt_template = cls.get_prompt("analyze")
            prompt = prompt_template.format(data=data_str)
            
            messages = [
                {"role": "system", "content": "你是一位资深的股票投资分析师，擅长技术分析和基本面分析。"},
                {"role": "user", "content": prompt}
            ]
            
            result = cls.call_deepseek_api(messages, temperature=0.7, max_tokens=2000)
            return result
            
        except Exception as e:
            logger.error(f"AI分析股票失败: {e}")
            raise
