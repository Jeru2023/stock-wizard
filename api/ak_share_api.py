import akshare as ak


class AKShareAPI:
    def __init__(self):
        pass

    @staticmethod
    def get_cn_tickers():
        df = ak.stock_info_a_code_name()
        df.rename(columns={"code": "symbol"}, inplace=True)
        df["exchange"] = ""  # 增加 exchange 字段
        df["ipo_date"] = ""  # 增加 ipo_date 字段
        return df

    @staticmethod
    def get_hk_tickers():
        df = ak.stock_hk_main_board_spot_em()
        df.rename(columns={"代码": "symbol", "名称": "name"}, inplace=True)
        df_selected = df[['symbol', 'name']]
        df_selected["exchange"] = "HKEX"  # 增加 exchange 字段
        df_selected["ipo_date"] = ""  # 增加 ipo_date 字段
        return df_selected

    def get_tickers(self, region):
        if region == 'cn':
            return self.get_cn_tickers()
        elif region == 'hk':
            return self.get_hk_tickers()
        else:
            raise ValueError(f"Invalid region: '{region}'.")
