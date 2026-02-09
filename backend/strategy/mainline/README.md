# 主线分析模块 (Mainline Analysis)

本目录包含市场核心主线识别的算法实现。

## 功能概述

该模块的主要目标是识别当前市场中资金最关注、赚钱效应最强的"主线板块"。

## 算法逻辑

### 1. 概念共振评分 (Resonance Scoring)

我们不单纯依赖指数涨跌，而是通过**板块内部共振**来判断强度。核心公式如下：

$$ Daily Score = AvgReturn 	imes 2.0 + LURatio 	imes 60.0 + Breadth 	imes 20.0 + \log(TotalAmount) 	imes 0.5 $$

其中：
- `AvgReturn`: 板块内个股平均涨幅。
- `LURatio` (Limit Up Ratio): 涨停家数占比。这是最核心的权重 (60.0)，因为主线必须有涨停潮。
- `Breadth`: 上涨家数占比（赚钱效应广度）。
- `TotalAmount`: 板块总成交额（反映资金容量）。

### 2. 行业共识算法 (Sector Consensus) - V9

解决"一只股票属于多个概念"的归类难题。

**问题**：协鑫集成既是"集成电路"（半导体），又是"光伏"（新能源）。如果简单归类，可能导致主线统计失真。

**解决方案**：
1.  **多维打分**：对一只股票的所有概念进行遍历。
2.  **权重加权**：根据 `config.py` 中的 `CATEGORY_WEIGHTS`，热门赛道（如低空经济、AI）拥有更高权重。
3.  **主要行业判定 (Primary Sector)**：将股票归属到得分最高的那个大类中。

## 文件说明

- `analyst.py`: 核心分析类 `MainlineAnalyst`，包含 `analyze` (单日分析) 和 `get_history` (历史演变) 方法。
- `config.py`: 包含概念映射表 (`CONCEPT_MAPPING`) 和权重表 (`CATEGORY_WEIGHTS`)。
