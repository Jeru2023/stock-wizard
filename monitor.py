import pandas as pd
from database import mydb
import mplfinance as mpf
from tools import utils
import os
from datetime import datetime



class Monitor:

    def __init__(self):
        # 获取当前日期
        current_date = datetime.now().date()
        # 将日期转换为字符串
        self.date_string = current_date.strftime('%Y-%m-%d')
        root_path = utils.get_root_path()
        self.parent_folder = os.path.join(root_path, 'output')
        self.output_folder = os.path.join(root_path, 'output', self.date_string)
        # 确保输出文件夹存在
        os.makedirs(self.output_folder, exist_ok=True)

    def plot_stocks_in_grid(self, dfs):
        for i, df in enumerate(dfs):
            symbol = df['symbol'].iloc[0]
            title = {"title": symbol, "y": 1}

            # 确保 Date 列为 datetime 格式
            df['date'] = pd.to_datetime(df['date'])

            # 设置 Date 列为索引
            df.set_index('date', inplace=True)

            # First we set the kwargs that we will use for all of these examples:
            kwargs = dict(type='candle', mav=(5, 20, 50), volume=True, figratio=(12, 8), figscale=1.5)
            save_path = os.path.join(self.output_folder, f'{symbol}.png')
            mpf.plot(df, **kwargs, title=title, style='checkers', savefig=save_path, tight_layout=True)

    def generate_html(self):
        # 获取所有图片文件
        images = [f for f in os.listdir(self.output_folder) if f.endswith('.png')]

        # 开始生成 HTML 内容
        html_content = f'''
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Stock Images</title>
                    <style>
                        body {{
                            display: flex;
                            flex-direction: column;
                            align-items: center;
                            padding: 20px;
                        }}
                        h1 {{
                            margin-bottom: 20px;
                            text-align: center;
                        }}
                        .image-container {{
                            margin: 10px;
                            width: calc(50% - 20px);
                            box-sizing: border-box;
                        }}
                        img {{
                            max-width: 100%;
                            height: auto;
                            display: block;
                        }}
                        .images {{
                            display: flex;
                            flex-wrap: wrap;
                            justify-content: center;
                            width: 100%;
                        }}
                    </style>
                </head>
                <body>
                <h1>{self.date_string} SEPA Screening Output [{len(images)} Found!]</h1>
                <div class="images">
                '''

        # 添加图片到 HTML
        for image in images:
            html_content += f'''
                    <div class="image-container">
                        <img src="{os.path.join(self.date_string, image)}" alt="{image}">
                    </div>
                    '''

        # 结束 HTML 内容
        html_content += '''
            </div>
        </body>
        </html>
        '''

        # 写入 HTML 文件
        html_file_path = os.path.join(self.parent_folder, 'index.html')
        with open(html_file_path, 'w') as html_file:
            html_file.write(html_content)

        print(f'HTML file generated at: {html_file_path}')

    def plot_all(self):
        df = mydb.get_screening_results(ma_200_up_trend=True, profit_up_trend=True, cup_with_handle=False)
        dfs = [group for _, group in df.groupby('symbol')]
        # 调用函数
        self.plot_stocks_in_grid(dfs)


if __name__ == '__main__':
    monitor = Monitor()
    #monitor.plot_all()
    monitor.generate_html()
