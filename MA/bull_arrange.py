import pandas as pd
import numpy as np
import copy
import mysql.connector
import re, time
import mydate
import mydb
import mytusharepro
import tradeday
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, ForeignKey
import my_email.sendmail as sm
import util.df_util as df_util
from concurrent.futures import ThreadPoolExecutor


def get_rolling_bull_arrange(ts_code, trade_date, m_days):
    engine = mydb.engine()
    conn = mydb.conn()
    cursor = conn.cursor()

    sql = str.format("show tables  like 'daily_{0}'", ts_code)
    cursor.execute(sql)
    table_exists = cursor.fetchall().__len__() > 0

    if not table_exists:
        return 0

    sql = """
    SELECT ma_10.trade_date,ma_10.ma ma_10,ma_20.ma ma_20,ma_30.ma ma_30,ma_60.ma ma_60,ma_100.ma ma_100,ma_250.ma ma_250,daily.close
    FROM ma_10 
    JOIN ma_20 ON ma_10.ts_code=ma_20.ts_code AND ma_10.trade_date=ma_20.trade_date
    JOIN ma_30 ON ma_10.ts_code=ma_30.ts_code AND ma_10.trade_date=ma_30.trade_date
    JOIN ma_60 ON ma_10.ts_code=ma_60.ts_code AND ma_10.trade_date=ma_60.trade_date
    JOIN ma_100 ON ma_10.ts_code=ma_100.ts_code AND ma_10.trade_date=ma_100.trade_date
    JOIN ma_250 ON ma_10.ts_code=ma_250.ts_code AND ma_10.trade_date=ma_250.trade_date
    JOIN `daily_{0}` daily ON ma_10.ts_code=daily.ts_code AND ma_10.trade_date=daily.trade_date
    AND ma_10.ts_code='{0}'
    ORDER BY ma_10.trade_date ;
    """
    sql = sql.format(ts_code)
    df = pd.read_sql(sql, conn)

    df["bull_arrange"] = (df.ma_250 < df.ma_100) & (df.ma_100 < df.ma_60) & (df.ma_60 < df.ma_30) & (
                df.ma_30 < df.ma_20) & (df.ma_20 < df.ma_10)
    df["rolling_bull_arrange"] = df.bull_arrange.rolling(window=m_days).sum()

    result = df.loc[df.trade_date == trade_date]
    if len(result) == 1:
        return result.rolling_bull_arrange.values[0]
    return 0


if __name__ == '__main__':
    today = time.strftime('%Y%m%d')
    # batch_query_top_n(50, '20000101', 20)
    # batch_query_top_n(120, '20000101', 20)
    # batch_query_top_n(250, '20000101', 20)
    # send_result_mail()
    print(get_rolling_bull_arrange('300037.SZ', '20191225', 25))
