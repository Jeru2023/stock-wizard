import yfinance as yf
import pandas as pd
import logging
import re
from tools import utils
import os
from database import mydb


class YahooAPI:

    def __init__(self):
        root_path = utils.get_root_path()
        self.error_log = os.path.join(root_path, 'log', 'yfinance_download_errors.log')
        # self.error_log = 'yfinance_download_errors.log'
        # 配置日志
        logging.basicConfig(
            filename=self.error_log,  # 日志文件名
            level=logging.ERROR,  # 记录错误及以上级别的信息
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def get_daily_prices_by_symbols(self, symbols, start_date, end_date):
        try:
            # 清空日志文件
            with open(self.error_log, 'w'):  # 以写模式打开文件会清空内容
                pass
            # 从 Yahoo API 下载数据
            df = yf.download(symbols, start=start_date, end=end_date, group_by='ticker')
        except Exception as e:
            print("exception: ", e)

        proxy_error_symbols, delisted_symbols, invalid_symbols = self.extract_delisted_symbols()
        print(f"\n{len(delisted_symbols)} delisted symbols found.")
        print(f"{len(invalid_symbols)} invalid symbols found.")
        print(f"{len(proxy_error_symbols)} proxy error symbols found.")

        # 更新数据库
        if len(delisted_symbols) > 0:
            mydb.update_ticker_status('Delisted', delisted_symbols)
        if len(invalid_symbols) > 0:
            mydb.update_ticker_status('Invalid', invalid_symbols)

        # 检查是否获取到数据
        if df.empty:
            return pd.DataFrame()  # 返回空 DataFrame
        # 转换格式
        result_list = []
        for symbol in symbols:
            try:
                # 提取单个 symbol 的数据
                symbol_df = df[symbol].copy()  # 提取 symbol 对应的数据
                # 添加一列 Symbol
                symbol_df['Symbol'] = symbol
                # 重置索引，将 Date 变为普通列
                symbol_df = symbol_df.reset_index()
                # 添加到列表
                result_list.append(symbol_df)
            except KeyError:
                print(f"Warning: No data found for symbol {symbol}. Skipping.")
                continue

        # 合并所有数据
        result_df = pd.concat(result_list)

        new_columns = {
            'Date': 'date',
            'Symbol': 'symbol',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
            'Adj Close': 'adj_close',
            # 添加更多列名
        }

        result_df.rename(columns=new_columns, inplace=True)

        return result_df

    @staticmethod
    def get_quarterly_growth(symbol):
        # 获取股票数据
        stock = yf.Ticker(symbol)

        current_net_income = -1
        growth_rate = -1

        # 获取季度财务数据
        quarterly_financials = stock.quarterly_financials
        try:
            # 提取最新四个季度的净利润
            net_income = quarterly_financials.loc['Normalized Income']

            # 确保有足够的数据
            if len(net_income) < 5:
                print("Not enough quarterly data to calculate growth.")
            else:
                current_quarter = net_income.index[0]
                previous_quarter = net_income.index[4]

                if net_income[previous_quarter] != 0:  # 避免除以零
                    growth_rate = ((net_income[current_quarter] - net_income[previous_quarter]) / net_income[
                        previous_quarter]) * 100
                    current_net_income = net_income[current_quarter]
                else:
                    print("Previous net income zero")
        except Exception as e:
            # 捕获异常并返回0
            print(f"{symbol} Exception: ", e)

        return current_net_income, growth_rate

    def extract_delisted_symbols(self):
        delisted_symbols = []
        proxy_error_symbols = []
        invalid_symbols = []

        # 读取日志文件
        with open(self.error_log, 'r') as file:
            for line in file:
                # 查找可能被 delisted 的符号
                delisted_match = re.search(r"\['(.*?)'\]: (YFPricesMissingError|YFTzMissingError).*delisted", line)
                if delisted_match:
                    delisted_symbols.append(delisted_match.group(1))

                # 查找可能是 proxy 错误的符号
                proxy_match = re.search(r"\['(.*?)'\]: ProxyError", line)
                if proxy_match:
                    proxy_error_symbols.append(proxy_match.group(1))

                # 查找可能是无效的符号
                invalid_match = re.search(r"\['(.*?)'\]: YFInvalidPeriodError", line)
                if invalid_match:
                    invalid_symbols.append(invalid_match.group(1))

        # 确保没有重合的符号
        proxy_error_symbols = [symbol for symbol in proxy_error_symbols if symbol not in delisted_symbols]
        proxy_error_symbols = [symbol for symbol in proxy_error_symbols if symbol not in invalid_symbols]

        return proxy_error_symbols, delisted_symbols, invalid_symbols


if __name__ == '__main__':
    yahoo_api = YahooAPI()
    yahoo_api.get_daily_prices_by_symbols(['AAPL','AMD','ADSE','JFR-R-W','JFBRW'], '2024-10-20', '2024-10-24')
    #print(yahoo_api.get_quarterly_growth('AAPL'))
    #print(yahoo_api.get_quarterly_growth('AAPL'))

    #print(yahoo_api.extract_delisted_symbols())

