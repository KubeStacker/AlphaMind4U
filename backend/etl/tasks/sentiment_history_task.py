
import logging
import pandas as pd
from db.connection import fetch_df
from strategy.sentiment.analyst import sentiment_analyst

logger = logging.getLogger(__name__)

class SentimentHistoryTask:
    """
    Sentiment History Backfill Task
    
    Re-runs the sentiment analysis for a specified date range (or all history)
    to populate the 'market_sentiment' table with new logic (continuous scores, derivatives).
    """
    
    def run(self, start_date='2024-01-01'):
        logger.info(f"Starting Sentiment History Backfill from {start_date}...")
        
        # 1. Get all trade dates with data
        query = f"""
        SELECT DISTINCT trade_date 
        FROM daily_price 
        WHERE trade_date >= '{start_date}' 
        ORDER BY trade_date ASC
        """
        df_dates = fetch_df(query)
        
        if df_dates.empty:
            logger.warning("No trade dates found.")
            return
        
        dates = df_dates['trade_date'].tolist()
        total = len(dates)
        
        logger.info(f"Found {total} trading days. Processing...")
        
        # 2. Sequential Processing
        # (Sequential is important because derivatives depend on previous day's score)
        for i, date_val in enumerate(dates):
            date_str = date_val.strftime('%Y-%m-%d')
            try:
                logger.info(f"[{i+1}/{total}] Analyzing {date_str}...")
                sentiment_analyst.analyze(date_str)
            except Exception as e:
                logger.error(f"Failed to analyze {date_str}: {e}")
        
        logger.info("Sentiment History Backfill Completed.")

if __name__ == "__main__":
    # Setup basic logging if run directly
    logging.basicConfig(level=logging.INFO)
    task = SentimentHistoryTask()
    task.run(start_date='2023-01-01')
