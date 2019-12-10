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
    a.extrs >750 
    group by trade_date,ts_code
    HAVING count(0)>1
    )a
    group BY a.ts_code, LEFT(a.trade_date,4)
    ) a join daily_basic b on a.trade_date=b.trade_date and a.ts_code=b.ts_code
    order by trade_date;
    """
    df = pd.read_sql(sql, conn)
    df = df_util.append_fina_indicator(df)
    df = append_price(df, 0)
    df = append_price(df, 100)
    df = track_n_percent(df, 5)
    df = track_n_percent(df, -5)
    df = track_n_percent(df, 10)
    df = track_n_percent(df, -10)
    df.to_csv('d:/temp/df.csv',encoding="utf_8_sig")


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
    column_min_or_max_rate = 'percent_%s_min_or_max_rate' % n_percent
    column_min_or_max_days = 'percent_%s_min_or_max_days' % n_percent
    column_days = 'percent_%s_days' % n_percent
    df_util.append_column(df, column_min_or_max_rate)
    df_util.append_column(df, column_min_or_max_days)
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
        daily_rows = pd.read_sql(sql, conn)

        min_or_max_price = close0
        min_or_max_days = 0
        for daily_index, daily_row in daily_rows.iterrows():
            daily_close = daily_row["close"]
            daily_date = daily_row["trade_date"]

            if n_percent<0:
                if daily_close > min_or_max_price:
                    min_or_max_price = daily_close
                    min_or_max_days = daily_index
            else:
                if daily_close < min_or_max_price:
                    min_or_max_price = daily_close
                    min_or_max_days = daily_index

            rate = (daily_close - close0) * 100 / close0
            found = (0 < n_percent <= rate) or (0 > n_percent >= rate)
            if found:
                df.loc[index, column_days] = daily_index
                df.loc[index, column_min_or_max_rate] = (min_or_max_price- close0) * 100 / close0
                df.loc[index, column_min_or_max_days] = min_or_max_days
                break

    return df


if __name__ == '__main__':
    analyze()




