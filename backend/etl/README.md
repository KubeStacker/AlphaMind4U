# ETL 模块说明

本模块负责从不同数据源（如 Tushare, Akshare）采集金融数据，并进行清洗、转换和存储。

## 目录结构

- `providers/`: 数据源适配器
    - `base.py`: 数据源抽象基类
    - `tushare_pro.py`: Tushare 实现
    - `akshare_pro.py`: Akshare 实现
- `tasks/`: 数据采集任务
    - `base_task.py`: 任务基类
    - `daily_price_task.py`: 日线行情同步
    - `stock_basic_task.py`: 股票基础信息同步
    - ...
- `sync.py`: 统一入口 (Facade)，对外提供 `sync_engine`
- `factory.py`: 数据源工厂，根据配置创建 Provider 实例
- `factors.py`: 技术因子计算逻辑 (SQL 实现)

## 数据源配置

默认使用 `Tushare`。如需切换到 `Akshare`，请在 `core/config.py` 或环境变量中设置 `DATA_SOURCE=akshare`。

### 字段映射

为保证上层应用透明，所有 Provider 返回的数据 DataFrame 必须包含标准化的字段名：
- `ts_code`: 股票代码 (000001.SZ)
- `trade_date`: 交易日期 (YYYY-MM-DD)
- `open`, `high`, `low`, `close`: 行情数据
- `vol`, `amount`: 成交量/额

## 同步策略

不同数据源支持不同的同步策略：
1. **Date-Oriented (日期优先)**: Tushare 支持按日期批量获取全市场数据，适合历史回溯。
2. **Ticker-Oriented (代码优先)**: Akshare 支持按个股获取历史序列，适合单个股票更新或非 Tushare 源的全量同步。

`DailyPriceTask` 会自动根据 Provider 的 `sync_mode` 选择最优同步循环。
