import pandas as pd
import mysql.connector
import re, time
import mydate
import mydb
import mytusharepro
import tradeday
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, ForeignKey

pro = mytusharepro.MyTusharePro()


def collect_daily_basic(last_date=None):
    engine = mydb.engine()
    conn = mydb.conn()
    # 获取元数据
    metadata = MetaData()
    # 定义表
    daily = Table('daily_basic', metadata,
                  Column('ts_code', String(20), primary_key=True),
                  Column('trade_date', String(20), primary_key=True, index=True),
                  Column('close', Float),
                  Column('turnover_rate', Float),
                  Column('turnover_rate_f', Float),
                  Column('volume_ratio', Float),
                  Column('pe', Float),
                  Column('pe_ttm', Float),
                  Column('pb', Float),
                  Column('ps', Float),
                  Column('ps_ttm', Float),
                  Column('total_share', Float),
                  Column('float_share', Float),
                  Column('free_share', Float),
                  Column('total_mv', Float),
                  Column('circ_mv', Float),
                  )
    # 创建数据表，如果数据表存在，则忽视
    metadata.create_all(engine)

    cursor = conn.cursor()
    if last_date is None:
        cursor.execute("select  max(trade_date) from daily_basic")
        last_date = cursor.fetchone()[0]

    if last_date is None:
        last_date = "20150101"

    last_date = mydate.string_to_next_day(last_date)
    # last_date = tradeday.get_next_tradeday(last_date)
    today = time.strftime('%Y%m%d')
    while last_date <= today:
        print("collect_daily_basic： %s" % last_date)
        cursor.execute("select count(0) from daily_basic where trade_date='" + last_date + "'")

        if cursor.fetchone()[0] == 0:
            daily_df = pro.daily_basic(trade_date=last_date)
            daily_df.to_sql('daily_basic', engine, index=False, if_exists='append')

        last_date = mydate.string_to_next_day(last_date)
        # last_date = tradeday.get_next_tradeday(last_date)


if __name__ == '__main__':
    collect_daily_basic()
