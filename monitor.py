import pandas as pd
from database import mydb
import mplfinance as mpf


def plot_stocks_in_grid(dfs):
    for i, df in enumerate(dfs):
        title = {"title": f"{df['symbol'].iloc[0]}", "y": 1}

        # 确保 Date 列为 datetime 格式
        df['date'] = pd.to_datetime(df['date'])

        # 设置 Date 列为索引
        df.set_index('date', inplace=True)

        # First we set the kwargs that we will use for all of these examples:
        kwargs = dict(type='candle', mav=(5, 20, 50), volume=True, figratio=(12, 8), figscale=1.5)
        mpf.plot(df, **kwargs, title=title, style='checkers', savefig='testsave.png', tight_layout=True )


df = mydb.get_screening_results(ma_200_up_trend=True, profit_up_trend=True, cup_with_handle=False)
print(len(df))
print(df.columns)

dfs = [group for _, group in df.groupby('symbol')]

# 调用函数
plot_stocks_in_grid(dfs[0:3])
