# /backend/strategy/recommend/engine.py

from .plugins.alpha_momentum import AlphaMomentumStrategy
from .plugins.backtester import BacktestPlugin

# 插件注册中心
# 这里注册了所有可用的推荐策略插件
_plugins = {
    "alpha_momentum": AlphaMomentumStrategy(),  # Alpha 动量策略
    "backtester": BacktestPlugin()              # 回测与验证插件
}

def get_plugin(name):
    """
    根据插件名称获取对应的策略实例
    """
    return _plugins.get(name)

def run_recommendation(strategy_id=None, **kwargs):
    """
    策略插件调度入口
    
    参数:
    - strategy_id: 策略 ID (默认为 'alpha_momentum')
    - kwargs: 传递给具体策略 run 方法的参数 (如 target_date, concept 等)
    
    返回:
    - 策略执行结果字典
    """
    sid = strategy_id or "alpha_momentum"
    plugin = _plugins.get(sid)
    
    if not plugin:
        return {"status": "error", "message": f"Strategy plugin '{sid}' not found."}
        
    return plugin.run(**kwargs)