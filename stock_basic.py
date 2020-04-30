import pandas as pd
import mysql.connector
import re, time
import mydate
import mydb
import mytusharepro
import tradeday
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, ForeignKey
from concurrent.futures import ThreadPoolExecutor
from NEWS.news_db import save_stock_basic_to_db

pro = mytusharepro.MyTusharePro()


def collect_stock_basic_work():
    stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    engine = mydb.engine()
    stock_basic.to_sql('stock_basic', engine, index=False, if_exists='replace')
    save_stock_basic_to_db(stock_basic)


def collect_stock_basic():
    engine = mydb.engine()
    conn = mydb.conn()
    # 获取元数据
    # metadata = MetaData()
    # # 定义表
    # daily = Table('stock_basic', metadata,
    #               Column('ts_code', String(20), primary_key=True),
    #               Column('symbol', String(200)),
    #               Column('name', String(200)),
    #               Column('area', String(200)),
    #               Column('industry', String(200)),
    #               Column('list_date', String(200)),
    #               )
    # # 创建数据表，如果数据表存在，则忽视
    # metadata.create_all(engine)

    collect_stock_basic_work()
    conn.close()


if __name__ == '__main__':
    collect_stock_basic()
