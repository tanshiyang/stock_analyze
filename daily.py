import pandas as pd
import mysql.connector
import re, time
import mydate
import mydb
import mytusharepro
import tradeday
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, ForeignKey

pro = mytusharepro.MyTusharePro()


def collect_daily(last_date=None):
    engine = mydb.engine()
    conn = mydb.conn()
    # 获取元数据
    metadata = MetaData()
    # 定义表
    daily = Table('daily', metadata,
                  Column('ts_code', String(20), primary_key=True),
                  Column('trade_date', String(20), primary_key=True, index=True),
                  Column('open', Float),
                  Column('high', Float),
                  Column('low', Float),
                  Column('close', Float),
                  Column('pre_close', Float),
                  Column('change', Float),
                  Column('pct_chg', Float),
                  Column('vol', Float),
                  Column('amount', Float),
                  )
    # 创建数据表，如果数据表存在，则忽视
    metadata.create_all(engine)

    cursor = conn.cursor()
    if last_date is None:
        cursor.execute("select  max(trade_date) from daily")
        last_date = cursor.fetchone()[0]

    if last_date is None:
        last_date = "20150101"

    last_date = mydate.string_to_next_day(last_date)
    # last_date = tradeday.get_next_tradeday(last_date)
    today = time.strftime('%Y%m%d')
    while last_date <= today:
        print("collect_daily： %s" % last_date)
        cursor.execute("select count(0) from daily where trade_date='" + last_date + "'")

        if cursor.fetchone()[0] == 0:
            daily_df = pro.daily(trade_date=last_date)
            daily_df.to_sql('daily', engine, index=False, if_exists='append')

        last_date = mydate.string_to_next_day(last_date)
        # last_date = tradeday.get_next_tradeday(last_date)


def collect_daily_qfq(start_date=None):
    engine = mydb.engine()
    conn = mydb.conn()
    cursor = conn.cursor()

    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
    today = time.strftime('%Y%m%d')
    for index, row in stocks.iterrows():
        ts_code = row["ts_code"]
        list_date = row["list_date"]
        table_name = "daily_" + str.lower(ts_code)
        print("collect_daily_qfq:"+ts_code)
        daily_df = pro.pro_bar(ts_code=ts_code, adj='qfq', start_date=list_date, end_date=today)

        # 获取元数据
        metadata = MetaData()
        # 定义表
        daily = Table(table_name, metadata,
                      Column('ts_code', String(20), primary_key=True),
                      Column('trade_date', String(20), primary_key=True, index=True),
                      Column('open', Float),
                      Column('high', Float),
                      Column('low', Float),
                      Column('close', Float),
                      Column('pre_close', Float),
                      Column('change', Float),
                      Column('pct_chg', Float),
                      Column('vol', Float),
                      Column('amount', Float),
                      )
        # 创建数据表，如果数据表存在，则忽视
        metadata.create_all(engine)

        if daily_df is None:
            continue
        if len(daily_df) == 0:
            continue

        cursor.execute("delete from `" + table_name + "`")
        conn.commit()
        daily_df.to_sql(table_name, engine, index=False, if_exists='append')


if __name__ == '__main__':
    collect_daily_qfq()
