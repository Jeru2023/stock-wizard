import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import configparser as cp
import logging
import os
from tools import utils

#####################################
# Database access module for stock code table and stock daily K line table
# usage:
# import mydb
# mydb.function_name(params)
#####################################

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.engine = self._create_engine()

    @staticmethod
    def _create_engine():
        config = cp.ConfigParser()
        config_path = os.path.join(utils.get_root_path(), 'config', 'stock.config')
        config.read(config_path, encoding='utf-8-sig')

        user = config.get('DB', 'user')
        password = config.get('DB', 'password')
        host = config.get('DB', 'host')
        port = config.get('DB', 'port')
        database = config.get('DB', 'database')

        return create_engine(
            'mysql+pymysql://{}:{}@{}:{}/{}'.format(user, password, host, port, database),
            pool_size=20,
            max_overflow=50,
            pool_recycle=30
        )

    def get_connection(self):
        return self.engine


# 实例化数据库连接
db = Database()


# query full stock list from DB
def query_all_tickers():
    engine = db.get_connection()
    sql = "select * from tickers where status='Active';"
    df = pd.read_sql_query(sql, engine)
    return df


def query_tickers_by_region(region):
    engine = db.get_connection()
    sql = "select * from tickers where region = '{}' and status='Active';".format(region)
    df = pd.read_sql_query(sql, engine)
    return df


def query_daily_stock_prices(symbol, start_date, end_date, mode='realtime'):
    table_name = 'daily_stock_prices_realtime'
    if mode == 'history':
        table_name = 'daily_stock_prices_history'

    if start_date is None:
        start_date = '2000-01-01'  # 默认从 2000-01-01 开始获取数据
    engine = db.get_connection()
    sql = ("select * from {} where symbol='{}' and date between '{}' and '{}' ORDER BY symbol, date;"
           .format(table_name, symbol, start_date, end_date))
    df = pd.read_sql_query(sql, engine)
    return df


def query_latest_daily_stock_prices(symbol, mode='realtime'):
    """
    查询某只股票的最新日期。
    """
    engine = db.get_connection()

    table_name = 'daily_stock_prices_realtime'
    if mode == 'history':
        table_name = 'daily_stock_prices_history'

    sql = f"""
    SELECT MAX(date) AS latest_date
    FROM {table_name}
    WHERE symbol = '{symbol}';
    """
    result = pd.read_sql_query(sql, engine)
    return result['latest_date'].iloc[0] if not result.empty else None


def calculate_moving_averages_batch():
    """
    批量计算所有股票的均线。
    """
    engine = db.get_connection()
    sql = """
       SELECT
    lp.symbol,
    lp.date,
    lp.close as current_price,
    (SELECT AVG(close)
     FROM (
         SELECT close
         FROM daily_stock_prices_realtime
         WHERE symbol = lp.symbol AND date <= lp.date
         ORDER BY date DESC
         LIMIT 50
     ) AS last_50_days) AS ma_50,
    (SELECT AVG(close)
     FROM (
         SELECT close
         FROM daily_stock_prices_realtime
         WHERE symbol = lp.symbol AND date <= lp.date
         ORDER BY date DESC
         LIMIT 150
     ) AS last_150_days) AS ma_150,
    (SELECT AVG(close)
     FROM (
         SELECT close
         FROM daily_stock_prices_realtime
         WHERE symbol = lp.symbol AND date <= lp.date
         ORDER BY date DESC
         LIMIT 200
     ) AS last_200_days) AS ma_200,
     (SELECT MAX(close)
     FROM daily_stock_prices_realtime
     WHERE symbol = lp.symbol) AS high_of_52weeks,
    (SELECT MIN(close)
     FROM daily_stock_prices_realtime
     WHERE symbol = lp.symbol) AS low_of_52weeks
FROM (
    SELECT
        dsp.symbol,
        dsp.date,
        dsp.close
    FROM
        daily_stock_prices_realtime dsp
    JOIN tickers AS t ON dsp.symbol = t.symbol
    WHERE
        t.status = 'Active'
        AND dsp.date = (SELECT MAX(date) FROM daily_stock_prices_realtime WHERE symbol = dsp.symbol)
) AS lp
ORDER BY
    lp.symbol;
        """
    return pd.read_sql_query(sql, engine)


def apply_sql_filter():
    engine = db.get_connection()
    sql = """
    SELECT symbol FROM daily_stock_moving_averages
    WHERE current_price > ma_50
    AND ma_50 > ma_150
    AND ma_150 > ma_200
    AND current_price >= low_of_52weeks * 1.3 
    AND current_price >= high_of_52weeks * 0.75;
    """
    df = pd.read_sql_query(sql, engine)
    return df


def get_screening_results():
    engine = db.get_connection()
    sql = """
    select dsp.date, dsp.symbol, dsp.close
    from daily_stock_prices_realtime as dsp
    join screening_output as so on so.symbol=dsp.symbol;
    """
    df = pd.read_sql_query(sql, engine)
    return df

def write_df_to_table(df, table_name):
    engine = db.get_connection()
    try:
        with engine.connect() as connection:
            logger.info("Database connection successful.")
            df.to_sql(table_name, connection, if_exists="append", index=False)
            logger.info(f"Data written to table {table_name} successfully.")
    except Exception as e:
        logger.error(f"Error writing to table {table_name}: {e}")


def update_inactive_tickers():
    engine = db.get_connection()
    sql = """
    UPDATE tickers
    SET status = 'inactive'
    WHERE symbol IN (
        SELECT symbol
        FROM daily_stock_prices_realtime
        GROUP BY symbol
        HAVING 
            DATEDIFF(CURDATE(), MAX(date)) > 30  -- 最后交易超过30天
            AND COUNT(*) >= 200  -- 总记录数超过200条
    );
    """
    with engine.connect() as connection:
        connection.execute(text(sql))
        connection.commit()


if __name__ == '__main__':
    apply_sql_filter()