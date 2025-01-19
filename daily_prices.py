from datetime import datetime, timedelta
from api.yahoo_api import YahooAPI
from tickers import Tickers
from database import mydb
import pandas as pd
import time


class DailyPrices:
    def __init__(self):
        self.yahoo_api = YahooAPI()
        self.tickers = Tickers()

    @staticmethod
    def filter_existing_data(df, symbol, start_date, end_date):
        # 查询数据库中该 symbol 的所有日期
        existing_dates = mydb.query_daily_stock_prices(symbol, start_date, end_date)['date'].tolist()
        # 将 df['Date'] 转换为 datetime.date 类型
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        # 过滤掉已经存在的日期
        return df[~df['Date'].isin(existing_dates)]

    def update_daily_prices_by_symbol(self, symbol, start_date=None, end_date=None):
        """
        更新指定 symbol 的每日价格数据，避免插入重复数据。

        :param symbol: 股票代码
        :param start_date: 数据开始日期（可选，默认为 None）
        :param end_date: 数据结束日期（可选，默认为当前日期）
        """
        # 如果没有提供 end_date，默认为当前日期
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        print(f"Updating daily prices for symbol {symbol} from {start_date} to {end_date}.")

        # 查询表中该 symbol 的最新日期
        try:
            latest_date = mydb.query_latest_daily_stock_prices(symbol)
            print(f"Latest date for symbol {symbol}: {latest_date}.")
            # 如果存在最新日期，则从最新日期的下一天开始获取数据
            start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
            print(f"Start date for symbol {symbol}: {start_date}.")
        except (IndexError, KeyError):
            # 如果表中没有该 symbol 的数据，则使用传入的 start_date 或默认值
            if start_date is None:
                start_date = '2000-01-01'  # 默认从 2000-01-01 开始获取数据

        # 如果 start_date 大于 end_date，说明数据已经是最新的，无需更新
        if start_date > end_date:
            print(f"No new data to update for symbol {symbol}.")
            return

        # 从 Yahoo API 获取数据
        df = self.yahoo_api.get_daily_prices_by_symbol(symbol, start_date, end_date)

        # 如果获取到数据，则写入数据库
        if not df.empty:
            # 确保日期列是字符串格式，并且不包含时区信息
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            # 过滤掉已经存在的数据
            df = self.filter_existing_data(df, symbol, None, end_date)

            try:
                mydb.write_df_to_table(df, 'daily_stock_prices')
                print(f"Updated {len(df)} rows of data for symbol {symbol}.")
            except Exception as e:
                print(f"Error updating data for symbol {symbol}: {e}")
        else:
            print(f"No new data available for symbol {symbol}.")

    def update_daily_prices(self):
        tickers = mydb.query_tickers_by_region('us')

        for index, row in tickers.iterrows():
            self.update_daily_prices_by_symbol(row['symbol'])
            print(f"[{index+1}]: Finished updating daily prices for symbol {row['symbol']}.")
            time.sleep(0.5)
        print("All symbols updated.")


if __name__ == '__main__':
    dp = DailyPrices()
    # dp.update_daily_prices_by_symbol('AAPL')
    dp.update_daily_prices()
