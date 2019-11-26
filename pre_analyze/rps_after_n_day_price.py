from pylab import *
import pandas as pd
import numpy as np
import copy
import mysql.connector
import re, time
import mydate
import mydb
import mytusharepro
import tradeday
import util.df_util as df_util

conn = mydb.conn()
cursor = conn.cursor()


def analyze():
    sql = """
    SELECT a.*,b.pe from (
    select min(trade_date) trade_date,min(ts_code) ts_code from (
    SELECT a.trade_date,ts_code,count(0) from rps_tops a where 
    a.extrs >800 
    group by trade_date,ts_code
    HAVING count(0)>1
    )a
    group by ts_code
    ) a join daily_basic b on a.trade_date=b.trade_date and a.ts_code=b.ts_code
    order by trade_date
    """
    df = pd.read_sql(sql, conn)
    df = append_price(df, 0)
    df = track_n_percent(df, 5)
    df = track_n_percent(df, -5)
    df.to_csv('d:/temp/df.csv')


def append_price(df, relative_days):
    column_name = 'close%s' % relative_days
    df_util.append_column(df, column_name)
    for index, row in df.iterrows():
        ts_code = row["ts_code"]
        trade_date = row["trade_date"]
        trade_date = mydate.string_to_relative_days(trade_date, relative_days)
        sql = str.format("show tables  like 'daily_{0}'", ts_code)
        cursor.execute(sql)
        table_exists = cursor.fetchall().__len__() > 0

        if not table_exists:
            continue

        sql = str.format("select close from `daily_{0}` where trade_date>='{1}' limit 1", ts_code, trade_date)
        cursor.execute(sql)
        price_row = cursor.fetchone()
        if price_row is not None:
            df.loc[index, column_name] = price_row[0]
    return df


def track_n_percent(df, n_percent):
    column_price = 'percent_%s_price' % n_percent
    column_date = 'percent_%s_date' % n_percent
    column_days = 'percent_%s_days' % n_percent
    df_util.append_column(df, column_price)
    df_util.append_column(df, column_date)
    df_util.append_column(df, column_days)
    for index, row in df.iterrows():
        ts_code = row["ts_code"]
        trade_date = row["trade_date"]
        close0 = row["close0"]
        sql = str.format("show tables  like 'daily_{0}'", ts_code)
        cursor.execute(sql)
        table_exists = cursor.fetchall().__len__() > 0

        if not table_exists:
            continue

        sql = str.format("select * from `daily_{0}` where trade_date>='{1}' order by trade_date", ts_code, trade_date)
        # cursor.execute(sql)
        daily_rows = pd.read_sql(sql, conn)
        # daily_rows = cursor.fetchall()
        for daily_index, daily_row in daily_rows.iterrows():
            daily_close = daily_row["close"]
            daily_date = daily_row["trade_date"]
            rate = (daily_close - close0) * 100 / close0
            found = (0 < n_percent <= rate) or (0 > n_percent >= rate)
            if found:
                df.loc[index, column_price] = daily_close
                df.loc[index, column_date] = daily_date
                df.loc[index, column_days] = daily_index
                break

    return df


if __name__ == '__main__':
    analyze()




