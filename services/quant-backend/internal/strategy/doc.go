// Package strategy 量化策略域：参数、股票池、因子、信号、组合、回测骨架。
//
// 分层顺序 params → universe → factors → signals → portfolio → backtest，
// 纯函数优先，DB 装配只在 universe 重建时触库；公式口径对齐 spec/strategy v0.4。
package strategy
