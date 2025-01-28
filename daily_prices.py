from datetime import datetime, timedelta
from api.yahoo_api import YahooAPI
from tickers import Tickers
from database import mydb
import pandas as pd
import time
import numpy as np
from scipy.stats import linregress


class DailyPrices:
    def __init__(self):
        self.yahoo_api = YahooAPI()
        self.tickers = Tickers()

    @staticmethod
    def filter_existing_data(df, symbol, start_date, end_date, mode='realtime'):
        # 查询数据库中该 symbol 的所有日期
        existing_dates = mydb.query_daily_stock_prices(symbol, start_date, end_date, mode)['date'].tolist()
        # 将 df['Date'] 转换为 datetime.date 类型
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        # 过滤掉已经存在的日期
        return df[~df['Date'].isin(existing_dates)]

    def update_daily_prices_by_symbol(self, symbol, start_date=None, end_date=None, mode='realtime'):
        """
        更新指定 symbol 的每日价格数据，避免插入重复数据。

        :param symbol: 股票代码
        :param start_date: 数据开始日期（可选，默认为 None）
        :param end_date: 数据结束日期（可选，默认为当前日期）
        :param mode: realtime or history
        """
        table_name = 'daily_stock_prices_realtime'
        if mode == 'history':
            table_name = 'daily_stock_prices_history'

        # 如果没有提供 end_date，默认为当前日期
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        print(f"Updating daily prices for symbol {symbol} from {start_date} to {end_date}.")

        # 查询表中该 symbol 的最新日期

        latest_date = mydb.query_latest_daily_stock_prices(symbol, mode)
        print(f"Latest date for symbol {symbol}: {latest_date}.")
        if start_date is None:
            if mode == 'history':
                start_date = '2000-01-01'  # 默认从 2000-01-01 开始获取数据
            else:
                # 获取当前日期
                current_date = datetime.now()
                # 倒推一年
                start_date = current_date - timedelta(days=365)
        else:
            # 如果存在最新日期，则从最新日期的下一天开始获取数据
            start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
            print(f"Start date for symbol {symbol}: {start_date}.")


        # 如果 start_date 大于 end_date，说明数据已经是最新的，无需更新
        if start_date > datetime.now():
            print(f"No new data to update for symbol {symbol}.")
            return

        # 从 Yahoo API 获取数据
        df = self.yahoo_api.get_daily_prices_by_symbol(symbol, start_date, end_date)

        # 如果获取到数据，则写入数据库
        if not df.empty:
            # 确保日期列是字符串格式，并且不包含时区信息
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            # 过滤掉已经存在的数据
            df = self.filter_existing_data(df, symbol, None, end_date, mode)

            try:
                mydb.write_df_to_table(df, table_name)
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

    @staticmethod
    def update_moving_averages():
        """
            批量更新所有股票的均线数据。
            """
        # 批量计算均线
        result_df = mydb.calculate_moving_averages_batch()
        print(f"Updating {len(result_df)} rows of moving averages.")

        # 将结果写入 daily_stock_moving_averages 表
        if not result_df.empty:
            mydb.write_df_to_table(result_df, 'daily_stock_moving_averages')

    @staticmethod
    def mark_inactive_tickers():
        """
        标记所有已经过期的股票。
        """
        mydb.update_inactive_tickers()

    @staticmethod
    def save_screening_output():
        df = mydb.apply_sql_filter()
        mydb.write_df_to_table(df, 'screening_output')

    @staticmethod
    def check_slope_trend(group):
        if len(group) < 20:
            return pd.Series({'slope': None, 'p_value': None, 'trend': False})

        x = np.arange(20)  # 时间序号0~19
        y = group['ma_200'].values
        slope, _, _, p_value, _ = linregress(x, y)

        return pd.Series({
            'slope': slope,
            'p_value': p_value,
            'trend': (slope > 0) & (p_value < 0.05)  # 斜率正且显著
        })

    @staticmethod
    def calculate_sma():
        df = mydb.get_screening_results()
        print(f"{len(df)} symbols found from first round screening")

        df = df.sort_values(['symbol', 'date']).reset_index(drop=True)

        df['ma_200'] = df.groupby('symbol')['close'].transform(
            lambda x: x.rolling(200, min_periods=200).mean()
        )
        df = df[df['ma_200'].notna()]
        return df

    @staticmethod
    def calculate_rsr(df, price_col='close', period=14):
        """
        计算相对动力排名 (Relative Strength Ranking, RSR)

        参数:
        - df: 包含价格数据的 DataFrame
        - price_col: 股票价格列名
        - period: 计算 RSR 的时间周期

        返回:
        - DataFrame: 包含 RSR 值的 DataFrame
        """
        # 计算过去一段时间的平均价格
        df['rolling_mean'] = df[price_col].rolling(window=period).mean()

        # 计算 RS
        df['rs'] = df[price_col] / df['rolling_mean']

        # 将 RS 标准化到 0-100 范围
        df['rsr'] = (df['rs'] - df['rs'].min()) / (df['rs'].max() - df['rs'].min()) * 100

        # 清理临时列
        df.drop(columns=['rolling_mean', 'rs'], inplace=True)

        return df

    def apply_final_filter(self):
        df = self.calculate_sma()
        recent_sma = df.groupby('symbol').tail(20)

        result = recent_sma.groupby('symbol').apply(self.check_slope_trend).reset_index()

        qualified_symbols = result[result['trend']]['symbol'].tolist()

        print("符合200日均线上升趋势股票数量:", len(qualified_symbols))

        # 根据 qualified_symbols 过滤原始 DataFrame
        filtered_df = df[df['symbol'].isin(qualified_symbols)]

        # 计算 RSR
        filtered_df = self.calculate_rsr(filtered_df)

        # 根据 RSR 进行最后一次过滤
        final_filtered_df = filtered_df[filtered_df['rsr'] >= 70]
        print("最终符合条件的股票数量:", len(final_filtered_df))
        print("最终符合条件的股票：", final_filtered_df['symbol'].unique().tolist())


if __name__ == '__main__':
    dp = DailyPrices()
    # dp.update_daily_prices_by_symbol('AAPL')
    #dp.update_daily_prices()

    # dp.mark_inactive_tickers()
    #dp.update_moving_averages()

    # dp.save_screening_output()

    dp.apply_final_filter()



