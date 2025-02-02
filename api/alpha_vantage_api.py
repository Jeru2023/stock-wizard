import configparser as cp
import pandas as pd
import numpy as np
import requests
import csv
from tools import utils
import os


class AlphaVantageAPI:
    def __init__(self):
        config = cp.ConfigParser()
        config_path = os.path.join(utils.get_root_path(), 'config','stock.config')
        config.read(config_path, encoding='utf-8-sig')
        self.base_url = config.get('AlphaVantage', 'base_url')
        self.api_key = config.get('AlphaVantage', 'api_key')

    @staticmethod
    def request_data(url):
        r = requests.get(url)
        data = r.json()
        return data

    def query_fundamental_by_symbol(self, function, symbol):
        url = '{}?function={}&symbol={}&apikey={}'.format(self.base_url, function, symbol, self.api_key)
        return self.request_data(url)

    def query_macro_indicator(self, function, interval=None, maturity=None):
        url = '{}?function={}&apikey={}'.format(self.base_url, function, self.api_key)
        if interval:
            url += '&interval={}'.format(interval)
        if maturity:
            url += '&maturity={}'.format(maturity)

        data = self.request_data(url)
        df = pd.DataFrame(data['data'])
        return df

    ############################
    #     fundamental APIs     #
    ############################

    def get_earnings(self, symbol):
        data = self.query_fundamental_by_symbol('EARNINGS', symbol)

        # 提取年度数据
        annual_earnings = data['annualEarnings']
        # 提取季度数据
        quarterly_earnings = data['quarterlyEarnings']

        # 将年度数据转换为 DataFrame
        df_annual = pd.DataFrame(annual_earnings)
        # 将季度数据转换为 DataFrame
        df_quarterly = pd.DataFrame(quarterly_earnings)

        # 将字符串 'None' 替换为 NaN
        df_quarterly['estimatedEPS'] = df_quarterly['estimatedEPS'].replace('None', np.nan)
        df_quarterly['surprise'] = df_quarterly['surprise'].replace('None', np.nan)
        df_quarterly['surprisePercentage'] = df_quarterly['surprisePercentage'].replace('None', np.nan)
        # 转换为 float 类型
        df_quarterly['estimatedEPS'] = df_quarterly['estimatedEPS'].astype(float)
        df_quarterly['surprise'] = df_quarterly['surprise'].astype(float)
        df_quarterly['surprisePercentage'] = df_quarterly['surprisePercentage'].astype(float)

        # 按照 fiscalDateEnding 列对年度数据进行降序排列
        df_annual = df_annual.sort_values(by='fiscalDateEnding', ascending=False)
        # 按照 fiscalDateEnding 列对季度数据进行降序排列
        df_quarterly = df_quarterly.sort_values(by='fiscalDateEnding', ascending=False)

        df_annual['Symbol'] = symbol
        df_quarterly['Symbol'] = symbol

        return df_annual, df_quarterly

    ############################
    #        macro APIs        #
    ############################

    def get_real_gdp(self, interval):
        df = self.query_macro_indicator('REAL_GDP', interval)
        return df

    def get_cpi(self, interval):
        df = self.query_macro_indicator('CPI', interval)
        return df

    def get_federal_funds_rate(self, interval):
        df = self.query_macro_indicator('FEDERAL_FUNDS_RATE', interval)
        return df

    def get_treasury_yield(self, interval, maturity):
        # By default, maturity=10year. Strings 3month, 2year, 5year, 7year, 10year, and 30year are accepted.
        df = self.query_macro_indicator('TREASURY_YIELD', interval, maturity)
        return df

    # annual only
    def get_annual_inflation(self):
        df = self.query_macro_indicator('INFLATION')
        return df

    # monthly only
    def get_monthly_unemployment(self):
        df = self.query_macro_indicator('UNEMPLOYMENT')
        return df

    # monthly only
    def get_monthly_retail_sales(self):
        df = self.query_macro_indicator('RETAIL_SALES')
        return df

    # monthly only
    # "unit": "thousands of persons"
    def get_monthly_nonfram_payroll(self):
        df = self.query_macro_indicator('NONFARM_PAYROLL', None)
        return df

    def get_tickers(self):
        url = '{}?function={}&apikey={}&state={}'.format(self.base_url, 'LISTING_STATUS', self.api_key, 'active')
        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            data = list(cr)
        # 将 CSV 数据转换为 DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])  # 第一行为列名

        # 只保留 symbol, name, exchange 三个字段
        df = df[['symbol', 'name', 'exchange', 'ipoDate', 'status']]
        df.rename(columns={"ipoDate": "ipo_date"}, inplace=True)

        return df


if __name__ == '__main__':
    av_api = AlphaVantageAPI()
    #print(av_api.get_tickers().head())

    # EARNINGS, INCOME_STATEMENT
    annual_earnings, quarterly_earnings = av_api.get_earnings('AAPL')
    # print(annual_earnings.head())
    # print(quarterly_earnings.head())

    # REAL_GDP - annual and quarterly
    # 'name': 'Real Gross Domestic Product', 'interval': 'quarterly', 'unit': 'billions of dollars'
    # real_gdp = av_api.get_real_gdp('quarterly')
    # print(real_gdp)

    # cpi = av_api.get_cpi('monthly')
    # print(cpi)

    # _data = av_api.get_monthly_trearsury_yield()
    # print(_data)
