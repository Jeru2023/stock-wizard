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

    def update_daily_prices_by_symbols(self, symbols, start_date=None, end_date=None, mode='realtime'):
        """
        更新指定多个 symbols 的每日价格数据，避免插入重复数据。

        :param symbols: 股票代码列表
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

        # 查询每个 symbol 的最新日期
        latest_dates = {symbol: mydb.query_latest_daily_stock_prices(symbol, mode) for symbol in symbols}

        # 从 Yahoo API 获取数据
        df = self.yahoo_api.get_daily_prices_by_symbols(symbols, start_date, end_date)

        if not df.empty:
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

            # 创建一个列表来存储每个符号的数据
            updated_data = []

            # 逐个符号处理数据
            for symbol in symbols:
                latest_date = latest_dates[symbol]
                if latest_date:
                    start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    start_date = '2000-01-01' if mode == 'history' else (datetime.now() - timedelta(days=365)).strftime(
                        '%Y-%m-%d')

                # 过滤已经存在的数据
                filtered_df = self.filter_existing_data(df[df['Symbol'] == symbol], symbol, None, end_date, mode)

                if not filtered_df.empty:
                    updated_data.append(filtered_df)

            # 合并所有更新的数据
            if updated_data:
                final_df = pd.concat(updated_data, ignore_index=True)
                try:
                    mydb.write_df_to_table(final_df, table_name)
                    print(f"Updated {len(final_df)} rows of data for the batch of symbols.")
                except Exception as e:
                    print(f"Error updating data for symbols: {e}")
            else:
                print("No new data available for any symbols.")
        else:
            print("No data retrieved from Yahoo API.")

    def update_daily_prices(self):
        tickers = mydb.query_tickers_by_region('us')
        symbol_list = tickers['symbol'].tolist()  # 获取所有符号

        # 分组批处理，每批500个tickers
        for i in range(0, len(symbol_list), 300):
            batch_symbols = symbol_list[i:i + 300]
            self.update_daily_prices_by_symbols(batch_symbols)  # 批量更新
            print(f"Finished updating daily prices for batch: {batch_symbols}.")
            time.sleep(0.5)  # 控制请求频率

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
            mydb.truncate_table('daily_stock_moving_averages')
            mydb.write_df_to_table(result_df, 'daily_stock_moving_averages')

    @staticmethod
    def mark_invalid_tickers():
        """
        标记所有inactive and exclude的股票。
        """
        mydb.update_inactive_tickers()
        mydb.update_exclude_tickers()

    @staticmethod
    def save_screening_output():
        df = mydb.apply_sql_filter()
        if not df.empty:
            print(f"{len(df)} rows inserted!")
            mydb.truncate_table('screening_output')
            mydb.write_df_to_table(df, 'screening_output')

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

    @staticmethod
    def calculate_sma():
        df = mydb.get_screening_results()

        df = df.sort_values(['symbol', 'date']).reset_index(drop=True)

        df['ma_200'] = df.groupby('symbol')['close'].transform(
            lambda x: x.rolling(200, min_periods=200).mean()
        )
        df = df[df['ma_200'].notna()]
        return df

    @staticmethod
    def check_slope_trend(group):
        if len(group) < 20:  # 确保每个symbol有足够数据
            return pd.Series({'trend': False})
        x = np.arange(20)  # 时间序号0~19
        y = group['ma_200'].values
        slope, _, _, p_value, _ = linregress(x, y)
        return pd.Series({
            'slope': slope,
            'p_value': p_value,
            'trend': (slope > 0) & (p_value < 0.05)  # 斜率正且显著
        })

    def apply_ma_200_up_trend_filter(self, p_value_threshold=0.05):
        df = self.calculate_sma()

        # 获取每个symbol最近20天的数据
        recent_sma = df.groupby('symbol').tail(20)

        # 计算每个symbol的ma_200斜率和p-value
        def calculate_slope(group):
            x = np.arange(len(group))  # 时间序列
            y = group['ma_200'].values  # ma_200 值
            slope, intercept, r_value, p_value, std_err = linregress(x, y)
            return pd.Series({'slope': slope, 'p_value': p_value})

        # 对每个symbol计算斜率和p-value
        slope_pvalue_df = recent_sma.groupby('symbol').apply(calculate_slope).reset_index()

        # 筛选出斜率显著为正的symbol
        up_trend_symbols = slope_pvalue_df[
            (slope_pvalue_df['slope'] > 0) & (slope_pvalue_df['p_value'] < p_value_threshold)
            ]['symbol'].tolist()

        print("符合200日均线上升趋势股票数量:", len(up_trend_symbols))
        if len(up_trend_symbols) > 0:
            mydb.update_ma_200_up_trend(up_trend_symbols)

    @staticmethod
    def detect_cup_with_handle(df, cup_duration=20, handle_duration=5, cup_depth=0.15, handle_depth=0.08):
        """
        从数据尾部向前搜索，识别最近20天内的杯柄形态。
        """
        df = df.sort_values('date', ascending=False).reset_index(drop=True)  # 按日期倒序排列

        # 仅检查最近60天数据（加快计算）
        df = df.head(60)

        # 寻找最近的杯底（局部低点）
        cup_bottom = df['close'].idxmin()

        # 杯底右侧的杯顶（局部高点）
        cup_top = df.loc[:cup_bottom, 'close'].idxmax()  # 杯顶在杯底左侧（因为数据是倒序）

        # 计算杯部参数
        cup_length = cup_bottom - cup_top  # 杯部长度（倒序索引）
        cup_retracement = (df.loc[cup_top, 'close'] - df.loc[cup_bottom, 'close']) / df.loc[cup_top, 'close']

        # 检查杯部条件
        if cup_length < cup_duration or cup_retracement > cup_depth:
            return False

        # 寻找柄部（杯顶右侧的回调）
        handle_start = cup_top
        handle_end = df.loc[:handle_start, 'close'].idxmin()  # 柄部低点

        # 计算柄部参数
        handle_length = handle_start - handle_end
        handle_retracement = (df.loc[handle_start, 'close'] - df.loc[handle_end, 'close']) / df.loc[
            handle_start, 'close']

        # 检查柄部条件
        if handle_length < handle_duration or handle_retracement > handle_depth:
            return False

        # --- 动态交易量阈值 ---
        vol_volatility = df['volume'].pct_change().std()
        breakout_multiplier = 1.2 + 0.3 * vol_volatility
        cup_volume_ratio = 0.7 - 0.2 * vol_volatility

        # 计算交易量指标
        cup_volume = df.loc[max(0, cup_bottom - 5):min(len(df) - 1, cup_bottom + 5), 'volume'].mean()
        handle_volume = df.loc[handle_end:handle_start, 'volume'].mean()
        breakout_volume = df.loc[handle_end, 'volume']

        # 交易量条件
        # if not (breakout_volume > handle_volume * breakout_multiplier and
        #         cup_volume < breakout_volume * cup_volume_ratio):
        #     return False

        return True

    def apply_cup_with_handle_symbols_filter(self):
        """
        找到符合杯柄形态的股票符号。

        :param df: 包含日期、收盘价和交易量的 DataFrame
        :return: 符合条件的股票符号列表
        """
        df = mydb.get_screening_results(ma_200_up_trend=True, profit_up_trend=True)
        print(len(df['symbol'].unique().tolist()))
        symbols_with_cup = []

        # 按 symbol 分组并检测
        for symbol, group in df.groupby('symbol'):
            if self.detect_cup_with_handle(group):
                symbols_with_cup.append(symbol)
        print("symbols with cup", symbols_with_cup)
        print("len of symbols", len(symbols_with_cup))

        if len(symbols_with_cup) > 0:
            mydb.update_cup_with_handle(symbols_with_cup)

    def apply_profit_up_trend_filter(self):
        df = mydb.get_screening_results(ma_200_up_trend=True)
        symbols_list = df['symbol'].unique().tolist()

        filtered_symbols = []
        for symbol in symbols_list:

            current_net_income, growth = self.yahoo_api.get_quarterly_growth(symbol)  # 获取最新季度的利润同比增长
            if (growth > 20) and (current_net_income > 0):  # 过滤条件：同比增长必须大于20%
                filtered_symbols.append(symbol)

        print("符合200日均线上升趋势且利润同比增长大于20%的股票数量:", len(filtered_symbols))
        if len(filtered_symbols) > 0:
            mydb.update_profit_up_trend(filtered_symbols)

    def apply_final_filter(self):
        self.apply_ma_200_up_trend_filter()
        # df = self.calculate_sma()
        # recent_sma = df.groupby('symbol').tail(20)
        #
        # result = recent_sma.groupby('symbol').apply(self.check_slope_trend).reset_index()
        #
        # qualified_symbols = result[result['trend']]['symbol'].tolist()
        #
        # print("符合200日均线上升趋势股票数量:", len(qualified_symbols))
        #
        # # 过滤符合条件的股票
        # filtered_symbols = []
        # for symbol in qualified_symbols:
        #     print(symbol)
        #     growth = self.yahoo_api.get_quarterly_growth(symbol)  # 获取最新季度的利润同比增长
        #     if growth > 20:  # 过滤条件：同比增长必须大于20%
        #         filtered_symbols.append(symbol)
        #
        # print("符合200日均线上升趋势且利润同比增长大于20%的股票数量:", len(filtered_symbols))
        # return filtered_symbols

        # # 根据 qualified_symbols 过滤原始 DataFrame
        # filtered_df = df[df['symbol'].isin(qualified_symbols)]
        #
        # # 计算 RSR
        # filtered_df = self.calculate_rsr(filtered_df)
        #
        # # 根据 RSR 进行最后一次过滤
        # final_filtered_df = filtered_df[filtered_df['rsr'] >= 70]
        # print("最终符合条件的股票数量:", len(final_filtered_df))
        # print("最终符合条件的股票：", final_filtered_df['symbol'].unique().tolist())


if __name__ == '__main__':
    dp = DailyPrices()
    # dp.update_daily_prices_by_symbol('AAPL')
    # dp.update_daily_prices()

    # dp.mark_invalid_tickers()
    #dp.update_moving_averages()

    # dp.save_screening_output()

    # dp.apply_ma_200_up_trend_filter()
    dp.apply_profit_up_trend_filter()
    #dp.apply_cup_with_handle_symbols_filter()



