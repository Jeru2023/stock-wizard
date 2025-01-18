import akshare as ak
import pandas as pd


class AKShareAPI:
    def __init__(self):
        pass

    @staticmethod
    def get_cn_tickers():
        df = ak.stock_info_a_code_name()
        df.rename(columns={"code": "symbol"}, inplace=True)
        return df

    @staticmethod
    def get_hk_tickers():
        df = ak.stock_hk_spot()
        df_selected = df[['symbol', 'name']]
        return df_selected

    def get_tickers(self, region):
        if region == 'cn':
            return self.get_cn_tickers()
        elif region == 'hk':
            return self.get_hk_tickers()
        else:
            raise ValueError(f"Invalid region: '{region}'.")
