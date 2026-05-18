# Tushare 数据获取技能

## 描述

封装 Tushare Pro 接口调用，处理积分检查、重试逻辑和数据同步。

## 使用场景

- 新增数据同步任务
- 调用 Tushare Pro 接口
- 处理积分不足的情况

## 调用方式

**模块路径：** `backend/etl/providers/`

**示例：**

```python
from backend.etl.providers.tushare import TushareProvider

provider = TushareProvider()
df = provider.get_daily_price(ts_code='000001.SZ', start_date='20250101')
```

## 积分限制

**当前状态：** Token 仅有 2000 积分

**无权限接口：** `stk_factor`、`daily_hf` 等高积分接口

**处理原则：**
- 实现前先确认接口所需积分
- 如遇积分不足，给出说明即可，不用强求验证
- 不要花费大量Token尝试绕过或寻找替代验证方案

## 重试逻辑

使用 `tenacity` 库实现重试：

```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_data():
    return provider.call_api()
```

## 参考实现

- `backend/etl/providers/tushare.py` - Tushare提供者
- `backend/etl/tasks/` - 各数据同步任务
- `docs/spec/concepts/01-core-constraints.md` - 核心约束
