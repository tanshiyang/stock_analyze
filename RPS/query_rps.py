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

def init_rps_top_table(tablename):
    engine = mydb.engine()
    # 获取元数据
    metadata = MetaData()
    # 定义表
    daily = Table(tablename, metadata,
                  Column('top_type', String(20), primary_key=True, index=True),
                  Column('trade_date', String(20), primary_key=True, index=True),
                  Column('ts_code', String(20), primary_key=True, index=True),
                  Column('extrs', Float),
                  )
    # 创建数据表，如果数据表存在，则忽视
    metadata.create_all(engine)

def query_top_n(m, trade_date, top_n):
    tablename = "rps_tops"
    init_rps_top_table(tablename)

    engine = mydb.engine()
    conn = mydb.conn()
    cursor = conn.cursor()

    df = pd.read_sql("SELECT 'm%s' as top_type, trade_date, ts_code, extrs from standardization_extrs_%s where "
                     "trade_date='%s' order by extrs desc LIMIT %s" % (m, m, trade_date,top_n), conn)

    cursor.execute("delete from %s where top_type='m%s' and trade_date='%s'" % (tablename, m, trade_date))
    conn.commit()
    df.to_sql(tablename, engine, index=False, if_exists='append')


def batch_query_top_n(m, last_date=None, top_n=20):
    tablename = "rps_tops"
    conn = mydb.conn()
    cursor = conn.cursor()
    if last_date is None:
        init_rps_top_table(tablename)
        cursor.execute("select  max(trade_date) from %s where top_type='m%s'" % tablename, m)
        last_date = cursor.fetchone()[0]

    if last_date is None:
        last_date = "20150101"

    last_date = mydate.string_to_next_day(last_date)
    today = time.strftime('%Y%m%d')
    while last_date <= today:
        print("normalization: %s" % last_date)
        query_top_n(m, last_date, top_n)
        last_date = mydate.string_to_next_day(last_date)


if __name__ == '__main__':
    #query_today_top(50, '20191113', 20)
    batch_query_top_n(50, '20191001')
    batch_query_top_n(120, '20191001')
    batch_query_top_n(250, '20191001')
