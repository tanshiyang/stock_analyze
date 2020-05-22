import traceback
import mytusharepro
from Stock import mydb
from util import mydate
import pandas as pd
import re, time
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import sessionmaker
from Fund.model import FundNav

pro = mytusharepro.MyTusharePro()


def collect_fund_nav():
    now = time.strftime('%Y%m%d', time.localtime(time.time()))

    # 连接数据库
    conn = mydb.conn()
    cursor = conn.cursor()

    cursor.execute(
        "create table if not exists fund_nav ("
        "ts_code varchar(20),"
        "ann_date varchar(20),"
        "end_date varchar(20),"
        "unit_nav float,"
        "accum_nav float,"
        "accum_div float,"
        "net_asset float,"
        "total_netasset float,"
        "unique(ts_code,end_date))")

    funds = pd.read_sql("select * from fund_basic", conn)
    today = time.strftime('%Y%m%d')
    for index, row in funds.iterrows():
        try:
            ts_code = row["ts_code"]
            collect_one(ts_code)


        except Exception as e:
            print(e)

    conn.close()
    cursor.close()


def collect_one(ts_code):
    df_nav = pro.fund_nav(ts_code=ts_code, fields='ts_code, ann_date, end_date, unit_nav, accum_nav, '
                                                  'accum_div, net_asset, total_netasset, adj_nav')
    engine = mydb.engine()
    conn = engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()

    exist_rows = pd.read_sql("select * from fund_nav where ts_code='{0}'".format(ts_code), conn)
    if len(exist_rows) >= len(df_nav):
        return

    df_temp = exist_rows.append(df_nav)
    df_nav = df_temp.drop_duplicates(subset=['ts_code', 'end_date'], keep=False)

    for index, row in df_nav.iterrows():
        nav = FundNav()
        nav.ts_code = row["ts_code"]
        nav.ann_date = row["ann_date"]
        nav.end_date = row["end_date"]
        nav.unit_nav = row["unit_nav"]
        nav.accum_nav = row["accum_nav"]
        nav.accum_div = row["accum_div"]
        nav.net_asset = row["net_asset"]
        nav.total_netasset = row["total_netasset"]
        nav.adj_nav = row["adj_nav"]
        check_exist_rows = session.query(FundNav).filter(and_(FundNav.ts_code == nav.ts_code,
                                                              FundNav.end_date == nav.end_date)).all()
        if len(check_exist_rows) == 0:
            print("insert {0},{1},{2}".format(nav.ts_code, nav.end_date))
            session.add(nav)
            session.commit()
        # else:
        #     print("skipped {0},{1},{2}".format(nav.ts_code, nav.end_date))
        
    conn.close()
    engine.dispose()


if __name__ == '__main__':
    collect_fund_nav()


