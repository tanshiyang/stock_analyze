import pandas as pd
import mysql.connector
import re, time
import mydate
import mydb
import mytusharepro
import tradeday
from sqlalchemy import create_engine, Table, Column, Integer, String,Float, MetaData, ForeignKey

pro = mytusharepro.MyTusharePro()

def collect_daily(last_date):
    engine = mydb.engine()
    conn = mydb.conn()
    # 获取元数据
    metadata = MetaData()
    # 定义表
    daily = Table('daily', metadata,
                 Column('ts_code', String(20), primary_key=True),
                 Column('trade_date', String(20), primary_key=True),
                 Column('open',Float ),
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
    if last_date == "":
        cursor.execute("select  max(trade_date) from daily")
        last_date = cursor.fetchone()[0]

    if last_date is None:
        last_date = "20150101"

    last_date = mydate.string_to_next_day(last_date)
    #last_date = tradeday.get_next_tradeday(last_date)
    today = time.strftime('%Y%m%d')
    while last_date <= today:
        print(last_date)
        cursor.execute("select count(0) from daily where trade_date='" + last_date+"'")

        if cursor.fetchone()[0] == 0:
            daily_df = pro.daily(trade_date=last_date)
            daily_df.to_sql('daily', engine, index=False, if_exists='append')

        last_date = mydate.string_to_next_day(last_date)
        #last_date = tradeday.get_next_tradeday(last_date)

if __name__ == '__main__':
    collect_daily("")

