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


def calc_rps(m):
    print('')

def init_table(tablename):
    engine = mydb.engine()
    # 获取元数据
    metadata = MetaData()
    # 定义表
    daily = Table(tablename, metadata,
                  Column('ts_code', String(20), primary_key=True),
                  Column('trade_date', String(20), primary_key=True),
                  Column('extrs', Float),
                  )
    # 创建数据表，如果数据表存在，则忽视
    metadata.create_all(engine)


def calc_uprate(m):
    tablename = 'extrs_%s' % m
    init_table(tablename)

    engine = mydb.engine()
    conn = mydb.conn()
    cursor = conn.cursor()

    stocks = pd.read_sql("select distinct ts_code from daily", conn)

    for index, row in stocks.iterrows():
        ts_code = row["ts_code"]
        print(ts_code)
        df = pd.read_sql("select ts_code,trade_date,close from daily where ts_code = '%s' order by trade_date" % ts_code, conn)

        c = df.close.to_list()
        c_ref_n = df.close.to_list()
        for i in range(m):
            c_ref_n.insert(0, np.nan)
            c_ref_n.pop(len(c_ref_n) - 1)

        up_rate = (np.array(c) - np.array(c_ref_n)) / np.array(c_ref_n)
        df = df.drop(columns=['close'])
        df['extrs'] = up_rate

        cursor.execute("delete from %s where ts_code='%s'" % (tablename, ts_code))
        conn.commit()
        df.to_sql(tablename, engine, index=False, if_exists='append')

def normalization(trade_date, m):
    tablename = 'standardization_extrs_%s' % m
    init_table(tablename)

    engine = mydb.engine()
    conn = mydb.conn()
    cursor = conn.cursor()

    df = pd.read_sql("select ts_code,trade_date,extrs from extrs_50 where trade_date='%s'" % trade_date, conn)
    if len(df) == 0:
        return
    if len(df[df['extrs'].isnull()].index) >= len(df):
        return
    ma = np.max(df.extrs)
    mi = np.min(df.extrs)
    up_rate_standardization = (df.extrs - mi) * 1000 / (ma - mi)
    df["extrs"] = up_rate_standardization
    cursor.execute("delete from %s where trade_date='%s'" % (tablename, trade_date))
    conn.commit()
    df.to_sql(tablename, engine, index=False, if_exists='append')

def batch_normalization(last_date, m):
    tablename = 'standardization_extrs_%s' % m
    conn = mydb.conn()
    cursor = conn.cursor()
    if last_date == "":
        cursor.execute("select  max(trade_date) from %s" % tablename)
        last_date = cursor.fetchone()[0]
    last_date = mydate.string_to_next_day(last_date)
    today = time.strftime('%Y%m%d')
    while last_date <= today:
        print(last_date)
        normalization(last_date, m)
        last_date = mydate.string_to_next_day(last_date)

if __name__ == '__main__':
    calc_uprate(50)
    batch_normalization('20150101', 50)
    print('')
