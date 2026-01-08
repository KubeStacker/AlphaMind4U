"""
数据采集模块 - 使用akshare获取股票和板块数据
"""
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from database import get_raw_connection
from config import Config
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCollector:
    def __init__(self):
        self.conn = None
    
    def get_connection(self):
        """获取数据库连接"""
        if self.conn is None or not self.conn.open:
            self.conn = get_raw_connection()
        return self.conn
    
    def collect_stock_daily_data(self):
        """采集最近3个月的股票日K数据"""
        try:
            logger.info("开始采集股票日K数据...")
            conn = self.get_connection()
            
            # 获取所有A股股票列表
            logger.info("获取A股股票列表...")
            stock_list = ak.stock_info_a_code_name()
            
            # 计算日期范围（最近3个月）
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=Config.STOCK_DATA_RETENTION_DAYS)).strftime('%Y%m%d')
            
            total_stocks = len(stock_list)
            logger.info(f"共需处理 {total_stocks} 只股票")
            
            success_count = 0
            error_count = 0
            
            for idx, row in stock_list.iterrows():
                try:
                    stock_code = row['code'] if 'code' in row else row[0]
                    stock_name = row['name'] if 'name' in row else row[1]
                    
                    # 获取股票日K数据
                    try:
                        df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", 
                                               start_date=start_date, end_date=end_date, adjust="")
                        if df.empty:
                            continue
                        
                        # 处理列名（akshare返回的列名可能不同）
                        if '日期' in df.columns:
                            df = df.rename(columns={
                                '日期': 'date',
                                '开盘': 'open',
                                '收盘': 'close',
                                '最高': 'high',
                                '最低': 'low',
                                '成交量': 'volume',
                                '成交额': 'amount',
                            })
                        elif 'date' not in df.columns:
                            # 尝试使用第一列作为日期
                            date_col = df.columns[0]
                            df = df.rename(columns={date_col: 'date'})
                            # 根据实际列数调整
                            if len(df.columns) >= 6:
                                df.columns = ['date', 'open', 'close', 'high', 'low', 'volume'] + list(df.columns[6:])
                        
                        df['stock_code'] = stock_code
                        df['trade_date'] = pd.to_datetime(df['date']).dt.date
                        
                        # 确保必要的列存在
                        if 'amount' not in df.columns:
                            df['amount'] = 0
                        
                        # 计算均线
                        df = df.sort_values('trade_date')
                        df['ma5'] = df['close'].rolling(window=5, min_periods=1).mean()
                        df['ma10'] = df['close'].rolling(window=10, min_periods=1).mean()
                        df['ma20'] = df['close'].rolling(window=20, min_periods=1).mean()
                        df['ma30'] = df['close'].rolling(window=30, min_periods=1).mean()
                        df['ma60'] = df['close'].rolling(window=60, min_periods=1).mean()
                        
                        # 保存到数据库
                        df_to_save = df[['stock_code', 'trade_date', 'open', 'close', 'high', 'low', 
                                        'volume', 'amount', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60']].copy()
                        df_to_save.columns = ['stock_code', 'trade_date', 'open_price', 'close_price', 
                                              'high_price', 'low_price', 'volume', 'amount', 
                                              'ma5', 'ma10', 'ma20', 'ma30', 'ma60']
                        
                        # 使用INSERT ... ON DUPLICATE KEY UPDATE
                        for _, row in df_to_save.iterrows():
                            cursor = conn.cursor()
                            sql = """
                            INSERT INTO stock_daily 
                            (stock_code, trade_date, open_price, close_price, high_price, low_price, 
                             volume, amount, ma5, ma10, ma20, ma30, ma60)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            open_price=VALUES(open_price),
                            close_price=VALUES(close_price),
                            high_price=VALUES(high_price),
                            low_price=VALUES(low_price),
                            volume=VALUES(volume),
                            amount=VALUES(amount),
                            ma5=VALUES(ma5),
                            ma10=VALUES(ma10),
                            ma20=VALUES(ma20),
                            ma30=VALUES(ma30),
                            ma60=VALUES(ma60)
                            """
                            cursor.execute(sql, tuple(row))
                            conn.commit()
                            cursor.close()
                        
                        # 更新股票基本信息
                        cursor = conn.cursor()
                        sql = """
                        INSERT INTO stock_info (stock_code, stock_name)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE stock_name=VALUES(stock_name)
                        """
                        cursor.execute(sql, (stock_code, stock_name))
                        conn.commit()
                        cursor.close()
                        
                        success_count += 1
                        
                        # 获取主力资金流入数据
                        try:
                            self.collect_capital_flow(stock_code, start_date, end_date)
                        except Exception as e:
                            logger.warning(f"获取 {stock_code} 资金流入数据失败: {e}")
                        
                        if idx % 100 == 0:
                            logger.info(f"进度: {idx}/{total_stocks}, 成功: {success_count}, 失败: {error_count}")
                        
                        # 避免请求过快
                        time.sleep(0.1)
                        
                    except Exception as e:
                        logger.warning(f"获取股票 {stock_code} 数据失败: {e}")
                        error_count += 1
                        continue
                        
                except Exception as e:
                    logger.error(f"处理股票数据时出错: {e}")
                    error_count += 1
                    continue
            
            logger.info(f"股票数据采集完成！成功: {success_count}, 失败: {error_count}")
            
        except Exception as e:
            logger.error(f"采集股票数据时出错: {e}", exc_info=True)
    
    def collect_capital_flow(self, stock_code, start_date, end_date):
        """采集主力资金流入数据"""
        try:
            conn = self.get_connection()
            
            # 尝试获取个股资金流数据
            try:
                # 使用akshare获取资金流向数据
                flow_data = ak.stock_individual_fund_flow_rank(indicator="今日")
                if flow_data is not None and not flow_data.empty:
                    # 筛选当前股票的数据
                    stock_flow = flow_data[flow_data['代码'] == stock_code] if '代码' in flow_data.columns else None
                    if stock_flow is not None and not stock_flow.empty:
                        today = datetime.now().date()
                        cursor = conn.cursor()
                        sql = """
                        INSERT INTO stock_capital_flow 
                        (stock_code, trade_date, main_net_inflow, super_large_inflow, large_inflow)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        main_net_inflow=VALUES(main_net_inflow),
                        super_large_inflow=VALUES(super_large_inflow),
                        large_inflow=VALUES(large_inflow)
                        """
                        # 根据实际列名调整
                        main_inflow = stock_flow.iloc[0].get('主力净流入', 0) if '主力净流入' in stock_flow.columns else 0
                        super_inflow = stock_flow.iloc[0].get('超大单净流入', 0) if '超大单净流入' in stock_flow.columns else 0
                        large_inflow = stock_flow.iloc[0].get('大单净流入', 0) if '大单净流入' in stock_flow.columns else 0
                        
                        cursor.execute(sql, (stock_code, today, main_inflow, super_inflow, large_inflow))
                        conn.commit()
                        cursor.close()
            except Exception as e:
                # 如果获取失败，记录但不中断主流程
                logger.debug(f"获取 {stock_code} 资金流数据失败: {e}")
                pass
                
        except Exception as e:
            logger.warning(f"采集资金流数据失败: {e}")
    
    def collect_sector_daily_data(self):
        """采集最近10天的板块量价数据"""
        try:
            logger.info("开始采集板块日K数据...")
            conn = self.get_connection()
            
            # 获取板块列表
            try:
                sector_df = ak.stock_board_industry_name_em()
                # 提取板块名称列表
                if isinstance(sector_df, pd.DataFrame) and '板块名称' in sector_df.columns:
                    sector_list = sector_df['板块名称'].tolist()
                elif isinstance(sector_df, pd.DataFrame):
                    sector_list = sector_df.iloc[:, 0].tolist()  # 使用第一列
                else:
                    sector_list = list(sector_df) if hasattr(sector_df, '__iter__') else []
            except Exception as e:
                logger.warning(f"无法获取板块列表: {e}，跳过板块数据采集")
                return
            
            if not sector_list:
                logger.warning("板块列表为空，跳过板块数据采集")
                return
            
            # 计算日期范围（最近10天）
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=Config.SECTOR_DATA_RETENTION_DAYS)).strftime('%Y%m%d')
            
            logger.info(f"共需处理 {len(sector_list)} 个板块")
            
            success_count = 0
            for sector_name in sector_list[:20]:  # 先处理前20个板块，避免时间过长
                try:
                    # 获取板块日K数据
                    sector_data = ak.stock_board_industry_hist_em(
                        symbol=sector_name, 
                        start_date=start_date.replace('-', ''),
                        end_date=end_date.replace('-', ''),
                        adjust=""
                    )
                    
                    if sector_data is None or sector_data.empty:
                        continue
                    
                    # 处理数据格式 - 处理不同的列名
                    if '日期' in sector_data.columns:
                        sector_data['trade_date'] = pd.to_datetime(sector_data['日期']).dt.date
                    else:
                        date_col = sector_data.columns[0]
                        sector_data['trade_date'] = pd.to_datetime(sector_data[date_col]).dt.date
                    
                    # 保存板块信息
                    cursor = conn.cursor()
                    sql = """
                    INSERT INTO sector_info (sector_code, sector_name)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE sector_name=VALUES(sector_name)
                    """
                    cursor.execute(sql, (sector_name, sector_name))
                    conn.commit()
                    cursor.close()
                    
                    # 保存板块日K数据
                    for _, row in sector_data.iterrows():
                        cursor = conn.cursor()
                        sql = """
                        INSERT INTO sector_daily 
                        (sector_code, trade_date, open_price, close_price, high_price, low_price, volume, amount, change_pct)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        open_price=VALUES(open_price),
                        close_price=VALUES(close_price),
                        high_price=VALUES(high_price),
                        low_price=VALUES(low_price),
                        volume=VALUES(volume),
                        amount=VALUES(amount),
                        change_pct=VALUES(change_pct)
                        """
                        # 处理不同的列名
                        open_price = row.get('开盘', row.get('open', 0))
                        close_price = row.get('收盘', row.get('close', 0))
                        high_price = row.get('最高', row.get('high', 0))
                        low_price = row.get('最低', row.get('low', 0))
                        volume = row.get('成交量', row.get('volume', 0))
                        amount = row.get('成交额', row.get('amount', 0))
                        change_pct = row.get('涨跌幅', row.get('change_pct', 0))
                        
                        cursor.execute(sql, (
                            sector_name,
                            row['trade_date'],
                            open_price,
                            close_price,
                            high_price,
                            low_price,
                            volume,
                            amount,
                            change_pct
                        ))
                        conn.commit()
                        cursor.close()
                    
                    success_count += 1
                    time.sleep(0.2)  # 避免请求过快
                    
                except Exception as e:
                    logger.warning(f"获取板块 {sector_name} 数据失败: {e}")
                    continue
            
            logger.info(f"板块数据采集完成！成功: {success_count}/{len(sector_list[:20])}")
            
        except Exception as e:
            logger.error(f"采集板块数据时出错: {e}", exc_info=True)
    
    def collect_hot_stocks(self):
        """采集热度榜数据（雪球和东财）"""
        try:
            logger.info("开始采集热度榜数据...")
            conn = self.get_connection()
            today = datetime.now().date()
            cursor = conn.cursor()
            
            # 先删除当天的数据，避免重复
            cursor.execute("DELETE FROM hot_stocks WHERE trade_date = %s", (today,))
            conn.commit()
            
            # 获取雪球热度榜
            xueqiu_data = None
            try:
                xueqiu_data = ak.stock_hot_rank_em()
                if xueqiu_data is not None and not xueqiu_data.empty:
                    for idx, row in xueqiu_data.head(100).iterrows():
                        cursor = conn.cursor()
                        sql = """
                        INSERT INTO hot_stocks (stock_code, stock_name, source, `rank`, volume, trade_date)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        # 尝试获取成交量（优先当天，否则最新）
                        volume = 0
                        try:
                            stock_code = row.get('代码', '')
                            # 先尝试获取当天的成交量
                            volume_query = "SELECT volume FROM stock_daily WHERE stock_code = %s AND trade_date = %s"
                            vol_cursor = conn.cursor()
                            vol_cursor.execute(volume_query, (stock_code, today))
                            vol_result = vol_cursor.fetchone()
                            if vol_result and vol_result[0]:
                                volume = int(vol_result[0])
                            else:
                                # 如果当天没有，获取最新的成交量
                                latest_query = "SELECT volume FROM stock_daily WHERE stock_code = %s ORDER BY trade_date DESC LIMIT 1"
                                vol_cursor.execute(latest_query, (stock_code,))
                                latest_result = vol_cursor.fetchone()
                                if latest_result and latest_result[0]:
                                    volume = int(latest_result[0])
                            vol_cursor.close()
                        except Exception as e:
                            logger.debug(f"获取成交量失败 {row.get('代码', '')}: {e}")
                            volume = 0
                        
                        cursor.execute(sql, (
                            row.get('代码', ''),
                            row.get('股票名称', ''),
                            'xueqiu',
                            idx + 1,
                            volume,
                            today
                        ))
                        conn.commit()
                        cursor.close()
                    logger.info(f"雪球热度榜采集完成，共 {len(xueqiu_data.head(100))} 条")
            except Exception as e:
                logger.warning(f"获取雪球热度榜失败: {e}")
            
            # 获取东财热度榜 - akshare可能没有专门的东财接口，使用雪球数据作为占位
            # 注意：如果需要真正的东财数据，需要找到对应的akshare接口或使用其他数据源
            try:
                # 尝试使用其他可能的接口
                try:
                    dongcai_data = ak.stock_hot_rank_latest_em()
                    # 检查数据格式是否可用
                    if dongcai_data is not None and not dongcai_data.empty and '代码' in dongcai_data.columns:
                        for idx, row in dongcai_data.head(100).iterrows():
                            cursor = conn.cursor()
                            sql = """
                            INSERT INTO hot_stocks (stock_code, stock_name, source, `rank`, volume, trade_date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            stock_code = row.get('代码', '') or row.get('股票代码', '')
                            stock_name = row.get('股票名称', '') or row.get('名称', '')
                            
                            # 获取成交量
                            volume = 0
                            try:
                                vol_query = "SELECT volume FROM stock_daily WHERE stock_code = %s AND trade_date = %s"
                                vol_cursor = conn.cursor()
                                vol_cursor.execute(vol_query, (stock_code, today))
                                vol_result = vol_cursor.fetchone()
                                if vol_result and vol_result[0]:
                                    volume = int(vol_result[0])
                                else:
                                    # 获取最新成交量
                                    latest_query = "SELECT volume FROM stock_daily WHERE stock_code = %s ORDER BY trade_date DESC LIMIT 1"
                                    vol_cursor.execute(latest_query, (stock_code,))
                                    latest_result = vol_cursor.fetchone()
                                    if latest_result and latest_result[0]:
                                        volume = int(latest_result[0])
                                vol_cursor.close()
                            except:
                                volume = 0
                            
                            cursor.execute(sql, (
                                stock_code,
                                stock_name,
                                'dongcai',
                                idx + 1,
                                volume,
                                today
                            ))
                            conn.commit()
                            cursor.close()
                        logger.info(f"东财热度榜采集完成，共 {len(dongcai_data.head(100))} 条")
                    else:
                        # 接口返回的数据格式不符合，使用雪球数据作为占位
                        logger.info("东财接口数据格式不符合，使用雪球数据作为占位")
                        if xueqiu_data is not None and not xueqiu_data.empty:
                            for idx, row in xueqiu_data.head(100).iterrows():
                                cursor = conn.cursor()
                                sql = """
                                INSERT INTO hot_stocks (stock_code, stock_name, source, `rank`, trade_date)
                                VALUES (%s, %s, %s, %s, %s)
                                """
                                cursor.execute(sql, (
                                    row.get('代码', ''),
                                    row.get('股票名称', ''),
                                    'dongcai',
                                    idx + 1,
                                    today
                                ))
                                conn.commit()
                                cursor.close()
                            logger.info(f"东财热度榜（使用雪球数据）采集完成，共 {len(xueqiu_data.head(100))} 条")
                except Exception as e:
                    logger.warning(f"尝试获取东财热度榜失败: {e}，使用雪球数据作为占位")
                    if xueqiu_data is not None and not xueqiu_data.empty:
                        for idx, row in xueqiu_data.head(100).iterrows():
                            cursor = conn.cursor()
                            sql = """
                            INSERT INTO hot_stocks (stock_code, stock_name, source, `rank`, trade_date)
                            VALUES (%s, %s, %s, %s, %s)
                            """
                            # 尝试获取当天的成交量
                            volume = 0
                            try:
                                volume_query = "SELECT volume FROM stock_daily WHERE stock_code = %s AND trade_date = %s"
                                vol_cursor = conn.cursor()
                                vol_cursor.execute(volume_query, (row.get('代码', ''), today))
                                vol_result = vol_cursor.fetchone()
                                if vol_result:
                                    volume = int(vol_result[0]) if vol_result[0] else 0
                                vol_cursor.close()
                            except:
                                volume = 0
                            
                            cursor.execute(sql, (
                                row.get('代码', ''),
                                row.get('股票名称', ''),
                                'dongcai',
                                idx + 1,
                                volume,
                                today
                            ))
                            conn.commit()
                            cursor.close()
                        logger.info(f"东财热度榜（使用雪球数据）采集完成，共 {len(xueqiu_data.head(100))} 条")
            except Exception as e:
                logger.error(f"处理东财热度榜数据时出错: {e}")
            
            logger.info("热度榜数据采集完成！")
            
        except Exception as e:
            logger.error(f"采集热度榜数据时出错: {e}", exc_info=True)
    
    def collect_gainers(self):
        """采集涨幅榜数据"""
        try:
            logger.info("开始采集涨幅榜数据...")
            conn = self.get_connection()
            today = datetime.now().date()
            
            # 获取涨幅榜
            gainers_data = ak.stock_zh_a_spot_em()
            if gainers_data is not None and not gainers_data.empty:
                # 按涨跌幅排序，取前100
                top_gainers = gainers_data.nlargest(100, '涨跌幅')
                
                for idx, row in top_gainers.iterrows():
                    cursor = conn.cursor()
                    sql = """
                    INSERT INTO gainers (stock_code, stock_name, change_pct, trade_date)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE change_pct=VALUES(change_pct)
                    """
                    cursor.execute(sql, (
                        row.get('代码', ''),
                        row.get('名称', ''),
                        row.get('涨跌幅', 0),
                        today
                    ))
                    conn.commit()
                    cursor.close()
            
            logger.info("涨幅榜数据采集完成！")
            
        except Exception as e:
            logger.error(f"采集涨幅榜数据时出错: {e}", exc_info=True)
    
    def clean_old_data(self):
        """清理过期数据"""
        try:
            logger.info("开始清理过期数据...")
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 清理股票日K数据（保留最近3个月）
            cutoff_date = (datetime.now() - timedelta(days=Config.STOCK_DATA_RETENTION_DAYS)).date()
            cursor.execute("DELETE FROM stock_daily WHERE trade_date < %s", (cutoff_date,))
            stock_deleted = cursor.rowcount
            
            # 清理资金流数据
            cursor.execute("DELETE FROM stock_capital_flow WHERE trade_date < %s", (cutoff_date,))
            flow_deleted = cursor.rowcount
            
            # 清理板块日K数据（保留最近10天）
            sector_cutoff_date = (datetime.now() - timedelta(days=Config.SECTOR_DATA_RETENTION_DAYS)).date()
            cursor.execute("DELETE FROM sector_daily WHERE trade_date < %s", (sector_cutoff_date,))
            sector_deleted = cursor.rowcount
            
            # 清理热度榜数据（保留最近7天）
            hot_cutoff_date = (datetime.now() - timedelta(days=7)).date()
            cursor.execute("DELETE FROM hot_stocks WHERE trade_date < %s", (hot_cutoff_date,))
            hot_deleted = cursor.rowcount
            
            # 清理涨幅榜数据（保留最近7天）
            cursor.execute("DELETE FROM gainers WHERE trade_date < %s", (hot_cutoff_date,))
            gainers_deleted = cursor.rowcount
            
            conn.commit()
            cursor.close()
            
            logger.info(f"数据清理完成！删除股票数据: {stock_deleted}, 资金流: {flow_deleted}, "
                       f"板块: {sector_deleted}, 热度榜: {hot_deleted}, 涨幅榜: {gainers_deleted}")
            
        except Exception as e:
            logger.error(f"清理数据时出错: {e}", exc_info=True)
    
    def close(self):
        """关闭数据库连接"""
        if self.conn and self.conn.open:
            self.conn.close()
