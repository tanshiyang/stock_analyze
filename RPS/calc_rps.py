import os
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

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
from concurrent.futures import ThreadPoolExecutor

pro = mytusharepro.MyTusharePro()


def init_extrs_table(tablename):
    engine = mydb.engine()
    # 获取元数据
    metadata = MetaData()
    # 定义表
    daily = Table(tablename, metadata,
                  Column('ts_code', String(20), primary_key=True, index=True),
                  Column('trade_date', String(20), primary_key=True),
                  Column('extrs', Float),
                  )
    # 创建数据表，如果数据表存在，则忽视
    metadata.create_all(engine)


def init_std_extrs_table(tablename):
    engine = mydb.engine()
    # 获取元数据
    metadata = MetaData()
    # 定义表
    daily = Table(tablename, metadata,
                  Column('ts_code', String(20), primary_key=True),
                  Column('trade_date', String(20), primary_key=True, index=True),
                  Column('extrs', Float),
                  )
    # 创建数据表，如果数据表存在，则忽视
    metadata.create_all(engine)


def calc_upgrate_work(m, ts_code):
    print(str.format("calc_uprate: m{0},{1}", m, ts_code))

    engine = mydb.engine()
    conn = mydb.conn()
    cursor = conn.cursor()

    tablename = 'extrs_%s' % m

    df = pd.read_sql(
        str.format("select ts_code,trade_date,close from `daily_{0}` where ts_code = '{0}' order by trade_date",
                   ts_code), conn)

    if len(df) == 0:
        return

    last_date = df.trade_date.max()
    today = time.strftime('%Y%m%d')

    # if last_date == today:
    #    continue

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


def calc_uprate(m):
    tablename = 'extrs_%s' % m
    init_extrs_table(tablename)

    # stocks = pd.read_sql("select distinct ts_code from daily", conn)
    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
    with ThreadPoolExecutor(20) as executor:
        for index, row in stocks.iterrows():
            ts_code = row["ts_code"]
            executor.submit(calc_upgrate_work, m, ts_code)


def np_normalization(data):
    data = data.rank()
    _range = np.max(data) - np.min(data)
    return (data - np.min(data)) / _range


def np_standardization(data):
    mu = np.mean(data, axis=0)
    sigma = np.std(data, axis=0)
    return (data - mu) / sigma


def normalization(trade_date, m):
    print(str.format("normalization: m{0},{1}", m, trade_date))

    tablename = 'standardization_extrs_%s' % m
    init_std_extrs_table(tablename)

    engine = mydb.engine()
    conn = mydb.conn()
    cursor = conn.cursor()

    df = pd.read_sql("select ts_code,trade_date,extrs from extrs_%s where trade_date='%s'" % (m, trade_date), conn)
    if len(df) == 0:
        return
    if len(df[df['extrs'].isnull()].index) >= len(df):
        return
    # ma = np.max(df.extrs)
    # mi = np.min(df.extrs)
    # up_rate_standardization = (df.extrs - mi) * 1000 / (ma - mi)
    # df["extrs"] = up_rate_standardization
    df["extrs"] = np_normalization(df["extrs"]) * 1000
    cursor.execute("delete from %s where trade_date='%s'" % (tablename, trade_date))
    conn.commit()
    df.to_sql(tablename, engine, index=False, if_exists='append')


def batch_normalization(m, last_date=None):
    tablename = 'standardization_extrs_%s' % m
    conn = mydb.conn()
    cursor = conn.cursor()
    if last_date is None:
        init_std_extrs_table(tablename)
        cursor.execute("select  max(trade_date) from %s" % tablename)
        last_date = cursor.fetchone()[0]

    if last_date is None:
        last_date = "20150101"

    last_date = mydate.string_to_next_day(last_date)
    today = time.strftime('%Y%m%d')
    with ThreadPoolExecutor(10) as executor:
        while last_date <= today:
            executor.submit(normalization, last_date, m)
            # normalization(last_date, m)
            last_date = mydate.string_to_next_day(last_date)


if __name__ == '__main__':
    # calc_uprate(50)
    # calc_uprate(120)
    # calc_uprate(250)
    batch_normalization(50, '20000101')
    batch_normalization(120, '20000101')
    batch_normalization(250, '20000101')
    # batch_normalization(50,'19950101')
