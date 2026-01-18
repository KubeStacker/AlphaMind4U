-- Add missing database indexes for performance optimization

-- Indexes for sheep_daily table
ALTER TABLE sheep_daily ADD INDEX idx_sheep_code (sheep_code);
ALTER TABLE sheep_daily ADD INDEX idx_trade_date (trade_date);
ALTER TABLE sheep_daily ADD INDEX idx_sheep_code_trade_date (sheep_code, trade_date);
ALTER TABLE sheep_daily ADD INDEX idx_rps_250 (rps_250);
ALTER TABLE sheep_daily ADD INDEX idx_vcp_factor (vcp_factor);
ALTER TABLE sheep_daily ADD INDEX idx_vol_ma_5 (vol_ma_5);

-- Indexes for sheep_money_flow table
ALTER TABLE sheep_money_flow ADD INDEX idx_sheep_code (sheep_code);
ALTER TABLE sheep_money_flow ADD INDEX idx_trade_date (trade_date);
ALTER TABLE sheep_money_flow ADD INDEX idx_sheep_code_trade_date (sheep_code, trade_date);
ALTER TABLE sheep_money_flow ADD INDEX idx_main_net_inflow (main_net_inflow);

-- Indexes for sheep_basic table
ALTER TABLE sheep_basic ADD INDEX idx_sheep_code (sheep_code);
ALTER TABLE sheep_basic ADD INDEX idx_sheep_name (sheep_name);
ALTER TABLE sheep_basic ADD INDEX idx_market (market);
ALTER TABLE sheep_basic ADD INDEX idx_industry (industry);

-- Indexes for concept_theme table
ALTER TABLE concept_theme ADD INDEX idx_concept_name (concept_name);
ALTER TABLE concept_theme ADD INDEX idx_source (source);

-- Indexes for sheep_concept_mapping table
ALTER TABLE sheep_concept_mapping ADD INDEX idx_sheep_code (sheep_code);
ALTER TABLE sheep_concept_mapping ADD INDEX idx_concept_id (concept_id);

-- Indexes for market_hot_rank table
ALTER TABLE market_hot_rank ADD INDEX idx_sheep_code (sheep_code);
ALTER TABLE market_hot_rank ADD INDEX idx_trade_date (trade_date);
ALTER TABLE market_hot_rank ADD INDEX idx_source_date (source, trade_date);
ALTER TABLE market_hot_rank ADD INDEX idx_rank (rank);

-- Indexes for strategy_recommendations table
ALTER TABLE strategy_recommendations ADD INDEX idx_user_id (user_id);
ALTER TABLE strategy_recommendations ADD INDEX idx_run_date (run_date);
ALTER TABLE strategy_recommendations ADD INDEX idx_sheep_code (sheep_code);
ALTER TABLE strategy_recommendations ADD INDEX idx_ts_code (ts_code);
ALTER TABLE strategy_recommendations ADD INDEX idx_is_verified (is_verified);
ALTER TABLE strategy_recommendations ADD INDEX idx_final_result (final_result);

-- Indexes for sheep_financials table
ALTER TABLE sheep_financials ADD INDEX idx_sheep_code (sheep_code);
ALTER TABLE sheep_financials ADD INDEX idx_report_date (report_date);

-- Indexes for sector_money_flow table
ALTER TABLE sector_money_flow ADD INDEX idx_sector_name (sector_name);
ALTER TABLE sector_money_flow ADD INDEX idx_trade_date (trade_date);
ALTER TABLE sector_money_flow ADD INDEX idx_sector_date (sector_name, trade_date);
ALTER TABLE sector_money_flow ADD INDEX idx_main_inflow (main_net_inflow);

-- Indexes for market_index_daily table
ALTER TABLE market_index_daily ADD INDEX idx_index_code (index_code);
ALTER TABLE market_index_daily ADD INDEX idx_trade_date (trade_date);
ALTER TABLE market_index_daily ADD INDEX idx_index_code_date (index_code, trade_date);