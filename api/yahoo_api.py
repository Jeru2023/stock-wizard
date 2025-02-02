import yfinance as yf
import pandas as pd


class YahooAPI:

    @staticmethod
    def get_daily_prices_by_symbols(symbols, start_date, end_date):
        # 从 Yahoo API 下载数据
        df = yf.download(symbols, start=start_date, end=end_date)

        # 检查是否获取到数据
        if df.empty:
            return pd.DataFrame()  # 返回空 DataFrame

        # 处理数据，去掉多级列索引
        df.columns = df.columns.droplevel(0)  # 去掉多级列索引
        df.reset_index(inplace=True)  # 重置索引以保留日期

        # 添加 symbol 列，使用合并而不是循环
        df['Symbol'] = df.index.map(lambda x: symbols[x // len(symbols)])  # 使用映射来添加符号列

        # 重命名列以匹配数据库表结构
        df.rename(columns={
            'Date': 'Date',
            # 'Adj Close': 'Adj_Close',  # 如果需要可以取消注释
        }, inplace=True)

        return df

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
                #raise ValueError("Not enough quarterly data to calculate growth.")
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


if __name__ == '__main__':
    # MDGL
    _df = YahooAPI.get_daily_prices_by_symbols(['AAPL'], '2024-10-20', '2024-10-24')
    print(_df.head())
    yahoo_api = YahooAPI()
    #print(yahoo_api.get_quarterly_growth('AAPL'))
    #print(yahoo_api.get_quarterly_growth('AAPL'))

