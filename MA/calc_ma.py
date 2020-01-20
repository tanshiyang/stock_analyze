import os
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

import re, time
import mydate
import pandas as pd
import mydb
import mytusharepro
from sqlalchemy import Table, Column, String, Float, MetaData
from concurrent.futures import ThreadPoolExecutor

pro = mytusharepro.MyTusharePro()


def init_ma_table(table_name):
    engine = mydb.engine()
    # 获取元数据
    metadata = MetaData()
    # 定义表
    daily = Table(table_name, metadata,
                  Column('ts_code', String(20), primary_key=True, index=True),
                  Column('trade_date', String(20), primary_key=True),
                  Column('ma', Float),
                  )
    # 创建数据表，如果数据表存在，则忽视
    metadata.create_all(engine)


def calc_ma_work(m, ts_code):
    print(str.format("calc_ma: m{0},{1}", m, ts_code))

    engine = mydb.engine()
    conn = mydb.conn()
    cursor = conn.cursor()

    table_name = 'ma_%s' % m
    today = time.strftime('%Y%m%d')

    last_ma_date = None
    cursor.execute("select max(trade_date) from ma_{0} where ts_code='{1}'".format(m, ts_code))
    result = cursor.fetchone()
    if result is not None:
        last_ma_date = result[0]

    sql = str.format("select ts_code,trade_date,close from `daily_{0}` where ts_code = '{0}' order by trade_date",
                     ts_code)
    if last_ma_date is not None:
        sql = str.format("select ts_code,trade_date,close from `daily_{0}` where ts_code = '{0}' and trade_date >{1} "
                         "order by trade_date", ts_code, mydate.string_to_relative_days(last_ma_date, -m * 2))

    df_daily = pd.read_sql(sql, conn)

    if len(df_daily) == 0:
        return

    if last_ma_date is None:
        last_ma_date = df_daily.trade_date.min()

    if last_ma_date is None:
        return
    if last_ma_date >= today is None:
        return

    ma_column_name = "ma".format(m)
    df_daily[ma_column_name] = df_daily.close.rolling(window=m).mean()
    df_daily = df_daily.drop(df_daily[df_daily.trade_date <= last_ma_date].index)
    df_daily = df_daily.drop(['close'], axis=1)

    cursor.execute("delete from %s where ts_code='%s' and trade_date>'%s'" % (table_name, ts_code, last_ma_date))
    conn.commit()
    df_daily.to_sql(table_name, engine, index=False, if_exists='append')
    conn.close()
    cursor.close()


def calc_ma(m):
    table_name = 'ma_%s' % m
    init_ma_table(table_name)

    # stocks = pd.read_sql("select distinct ts_code from daily", conn)
    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
    with ThreadPoolExecutor(20) as executor:
        for index, row in stocks.iterrows():
            ts_code = row["ts_code"]
            executor.submit(calc_ma_work, m, ts_code)


if __name__ == '__main__':
    # calc_ma_work(10,'000055.SZ')
    # 10　20　30　60　100　250
    calc_ma(10)
    calc_ma(20)
    calc_ma(30)
    calc_ma(60)
    calc_ma(100)
    calc_ma(250)
    print('')
