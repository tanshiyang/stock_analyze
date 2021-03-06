import os
import sys

import util.df_appender

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

import pandas as pd
import time
from Stock import mydb
from util import mydate
import util.df_util as df_util


def analyze():
    conn = mydb.conn()
    cursor = conn.cursor()
    today = time.strftime('%Y%m%d')
    sql = """
    SELECT a.*,b.pe from (
    select min(trade_date) trade_date,min(ts_code) ts_code from (
    SELECT a.trade_date,ts_code,count(0) FROM 
	 (SELECT * FROM standardization_extrs_50 WHERE extrs>870
	  UNION SELECT * FROM standardization_extrs_120 WHERE extrs>870
	  UNION SELECT * FROM standardization_extrs_250 WHERE extrs>870
	 ) a 
    group by trade_date,ts_code
    HAVING trade_date > '20150101' and COUNT(0)>1
    )a
    group BY a.ts_code, LEFT(a.trade_date,6)
    ) a join daily_basic b on a.trade_date=b.trade_date and a.ts_code=b.ts_code
    AND LEFT(a.trade_date,4)-LEFT((select MIN(trade_date) FROM daily_basic WHERE ts_code = a.ts_code),4)>2
    order by trade_date;
    """
    df = pd.read_sql(sql, conn)

    # 30天内同一代码出现的次数
    df["ts_code_int"] = df.ts_code.str[:6]
    df.ts_code_int.to_numpy(int)
    df["rolling_appear_count"] = df.ts_code_int.rolling(window=30).apply(rolling_appear_count, raw=False)
    df = df.query("rolling_appear_count == 0")
    df.drop(['ts_code_int', 'rolling_appear_count'], axis=1, inplace=True)
    file_name = "d:/temp/df{0}-01.csv".format(today)
    df.to_csv(file_name, encoding="utf_8_sig")

    df = append_price(df, 0)
    df = util.df_appender.append_fina_indicator(df)
    file_name = "d:/temp/df{0}-02.csv".format(today)
    df.to_csv(file_name, encoding="utf_8_sig")

    df = util.df_appender.append_ma_bull_arrange(df)
    file_name = "d:/temp/df{0}-03.csv".format(today)
    df.to_csv(file_name, encoding="utf_8_sig")

    df = track_n_percent(df, 10)
    # df = track_n_percent(df, -5)
    # df = track_n_percent(df, 30)
    # df = track_n_percent(df, -10)
    file_name = "d:/temp/df{0}-result.csv".format(today)
    df.to_csv(file_name, encoding="utf_8_sig")
    conn.close()
    cursor.close()


def analyze_csv(file_name):
    df = pd.read_csv(file_name)
    df = append_price(df, 120)
    today = time.strftime('%Y%m%d%H%M')
    file_name = "d:/temp/df{0}.csv".format(today)
    df.to_csv(file_name, encoding="utf_8_sig")

def append_price(df, relative_days):
    conn = mydb.conn()
    cursor = conn.cursor()
    column_name = '收盘价%s' % relative_days
    df_util.append_column(df, column_name)
    for index, row in df.iterrows():
        ts_code = row["ts_code"]
        trade_date = str(row["trade_date"])
        trade_date = mydate.string_to_relative_days(trade_date, relative_days)
        print("append_price,{0},{1}".format(ts_code,trade_date))
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
    conn.close()
    cursor.close()
    return df


def rolling_appear_count(s):
    index = s.index[-1]
    value = s.values[-1]
    s.pop(index)
    return len(s[s == value])


def track_n_percent(df, n_percent):
    conn = mydb.conn()
    cursor = conn.cursor()
    column_target_days = '%s目标天数' % n_percent
    column_max_rate = '%s最大涨幅' % n_percent
    column_max_days = '%s最大涨幅用时' % n_percent
    column_min_rate = '%s最大跌幅' % n_percent
    column_min_days = '%s最大跌幅用时' % n_percent
    df_util.append_column(df, column_target_days)
    df_util.append_column(df, column_max_rate)
    df_util.append_column(df, column_max_days)
    df_util.append_column(df, column_min_rate)
    df_util.append_column(df, column_min_days)
    for index, row in df.iterrows():
        ts_code = row["ts_code"]
        trade_date = row["trade_date"]
        close0 = row["收盘价0"]
        sql = str.format("show tables  like 'daily_{0}'", ts_code)
        cursor.execute(sql)
        table_exists = cursor.fetchall().__len__() > 0

        if not table_exists:
            continue

        sql = str.format("select * from `daily_{0}` where trade_date>='{1}' order by trade_date", ts_code, trade_date)
        daily_rows = pd.read_sql(sql, conn)

        max_price = min_price = close0
        max_days = min_days = 0
        for daily_index, daily_row in daily_rows.iterrows():
            daily_close = daily_row["close"]
            daily_date = daily_row["trade_date"]

            if daily_close > max_price:
                max_price = daily_close
                max_days = daily_index

            if daily_close < min_price:
                min_price = daily_close
                min_days = daily_index

            rate = (daily_close - close0) * 100 / close0
            found = (0 < n_percent <= rate) or (0 > n_percent >= rate)
            if found:
                df.loc[index, column_target_days] = daily_index
                df.loc[index, column_max_rate] = (max_price - close0) * 100 / close0
                df.loc[index, column_max_days] = max_days
                df.loc[index, column_min_rate] = (min_price - close0) * 100 / close0
                df.loc[index, column_min_days] = min_days
                break
    conn.close()
    cursor.close()
    return df


if __name__ == '__main__':
    analyze()
    # analyze_csv("D:\\Temp\\df20200122-result.csv")
