import yfinance as yf
import pandas as pd


class YahooAPI:

    @staticmethod
    def get_daily_by_symbol(symbol, start_date, end_date):
        df = yf.download(symbol, start_date, end_date)
        df.columns = df.columns.droplevel('Ticker')

        # 重命名列以匹配数据库表结构
        df.reset_index(inplace=True)  # 重置索引以保留日期
        df.rename(columns={
            'Price Date': 'Date',
            'Adj Close': 'Adj_Close',
        }, inplace=True)

        # 添加 symbol 列
        df['Symbol'] = symbol

        return df


if __name__ == '__main__':
    _df = YahooAPI.get_daily_by_symbol('AAPL', '2024-10-20', '2024-10-24')
    print(_df.head())

