from __future__ import annotations


def build_strategy_summary_text(summary: dict) -> str:
    observation_count = int(summary.get("observation_count") or 0)
    completed_5d = int(summary.get("completed_count_5d") or 0)
    win_rate_5d = float(summary.get("win_rate_5d") or 0.0)
    avg_ret_5d = float(summary.get("avg_ret_5d") or 0.0)
    avg_drawdown_5d = float(summary.get("avg_max_drawdown_5d") or 0.0)

    if observation_count <= 0:
        return "暂无观察样本。"
    if completed_5d <= 0:
        return f"近窗共 {observation_count} 条观察，5日回测尚未补齐。"

    sign = "+" if avg_ret_5d >= 0 else ""
    return (
        f"近窗共 {observation_count} 条观察，5日完成 {completed_5d} 条，"
        f"5日胜率 {win_rate_5d:.1f}%，5日均值 {sign}{avg_ret_5d:.2f}%，"
        f"回撤 {avg_drawdown_5d:.2f}%。"
    )
