import pandas as pd
from api.alpha_vantage_api import AlphaVantageAPI
from api.ak_share_api import AKShareAPI
from database import mydb


class Tickers:
    def __init__(self):
        self.av_api = AlphaVantageAPI()
        self.ak_api = AKShareAPI()

    @staticmethod
    def _get_new_tickers(existing_tickers, new_tickers):
        """
        获取增量部分的新 ticker。
        """
        # 将 existing_tickers 的 symbol 转换为集合
        existing_symbols = set(existing_tickers['symbol'])

        # 过滤出 new_tickers 中不在 existing_tickers 中的行
        new_tickers_filtered = new_tickers[~new_tickers['symbol'].isin(existing_symbols)]

        return new_tickers_filtered

    def update_tickers_by_region(self, region):
        """
        根据 region 更新 tickers 表。
        """
        # 获取现有 tickers 数据
        existing_tickers = mydb.query_tickers_by_region(region)

        # 根据 region 获取新的 tickers 数据
        if region == 'us':
            new_tickers = self.av_api.get_tickers()
        else:
            new_tickers = self.ak_api.get_tickers(region)
        print(f"Got {len(new_tickers)} new tickers for region '{region}'.")

        # 获取增量部分的新 ticker
        new_tickers_filtered = self._get_new_tickers(existing_tickers, new_tickers)

        if not new_tickers_filtered.empty:
            # 添加 region 字段
            new_tickers_filtered["region"] = region

            # 只保留需要的字段：symbol, name, region, exchange, ipo_date
            new_tickers_filtered = new_tickers_filtered[["symbol", "name", "region", "exchange", "ipo_date", "status"]]
            # 将 ipo_date 字段的空字符串替换为 NULL
            new_tickers_filtered["ipo_date"] = new_tickers_filtered["ipo_date"].replace('', None)

            # 将增量数据插入数据库
            mydb.write_df_to_table(new_tickers_filtered, "tickers")
            print(f"Added {len(new_tickers_filtered)} new tickers for region '{region}'.")
        else:
            print(f"No new tickers to add for region '{region}'.")

    def update_tickers(self, region_list):
        """
        根据 region_list 更新 tickers 表。
        """
        for region in region_list:
            print(f"Updating tickers for region: {region}")
            self.update_tickers_by_region(region)
            print(f"Finished updating tickers for region: {region}")
        print("All regions updated.")


if __name__ == '__main__':
    ticker = Tickers()
    # ticker.update_tickers(['cn', 'hk', 'us'])
    ticker.update_tickers(['us'])
