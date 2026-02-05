# /backend/strategy/plugins/__init__.py

from .alpha_momentum import AlphaMomentumStrategy
from .backtester import BacktestPlugin

# 插件注册中心
_plugins = {
    "alpha_momentum": AlphaMomentumStrategy(),
    "backtester": BacktestPlugin()
}

def get_plugin(name):
    return _plugins.get(name)

def run_recommendation(strategy_id=None, **kwargs):
    """
    策略插件调度入口
    """
    sid = strategy_id or "alpha_momentum"
    plugin = _plugins.get(sid)
    
    if not plugin:
        return {"status": "error", "message": f"Strategy plugin '{sid}' not found."}
        
    return plugin.run(**kwargs)
