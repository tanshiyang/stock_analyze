import traceback
import mytusharepro
from Stock import mydb
from util import mydate
import pandas as pd
import re, time
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import sessionmaker
from Fund.model import FundManager
import numpy as np

pro = mytusharepro.MyTusharePro()


def collect_fund_manager():
    now = time.strftime('%Y%m%d', time.localtime(time.time()))

    # 连接数据库
    conn = mydb.conn()
    cursor = conn.cursor()

    cursor.execute(
        "create table if not exists fund_manager ("
        "ts_code varchar(20),"
        "ann_date varchar(20),"
        "name varchar(20),"
        "gender varchar(20),"
        "birth_year varchar(20),"
        "edu varchar(20),"
        "nationality varchar(20),"
        "begin_date varchar(20),"
        "end_date varchar(20),"
        "resume varchar(20),"
        "unique(ts_code,ann_date))")

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
    print("fund manager {0}".format(ts_code))
    df_nav = pro.fund_manager(ts_code=ts_code, fields='ts_code, ann_date, name, gender, birth_year, '
                                                      'edu, nationality, begin_date, end_date, resume')
    engine = mydb.engine()
    conn = engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()

    exist_rows = pd.read_sql("select * from fund_manager where ts_code='{0}'".format(ts_code), conn)
    if len(exist_rows) >= len(df_nav):
        return

    df_temp = exist_rows.append(df_nav)
    df_nav = df_temp.drop_duplicates(subset=['ts_code', 'ann_date'], keep=False)

    for index, row in df_nav.iterrows():
        manager = FundManager()
        manager.ts_code = row["ts_code"]
        manager.ann_date = row["ann_date"]
        manager.name = row["name"]
        manager.gender = row["gender"]
        manager.birth_year = row["birth_year"]
        manager.edu = row["edu"]
        manager.nationality = row["nationality"]
        manager.begin_date = row["begin_date"]
        manager.end_date = row["end_date"]
        manager.end_date = row["end_date"]

        check_exist_rows = session.query(FundManager).filter(and_(FundManager.ts_code == manager.ts_code,
                                                                  FundManager.ann_date == manager.ann_date)).all()
        if len(check_exist_rows) == 0:
            print("insert fund manager {0},{1}".format(manager.ts_code, manager.ann_date))
            session.add(manager)
            session.commit()
        else:
            print("skipped {0},{1}".format(manager.ts_code, manager.ann_date))

    conn.close()
    engine.dispose()


if __name__ == '__main__':
    collect_fund_manager()
