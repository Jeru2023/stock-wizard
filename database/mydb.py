import pandas as pd
from sqlalchemy import create_engine
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
    sql = "select * from tickers;"
    df = pd.read_sql_query(sql, engine)
    return df


def query_tickers_by_region(region):
    engine = db.get_connection()
    sql = "select * from tickers where region = '{}';".format(region)
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
