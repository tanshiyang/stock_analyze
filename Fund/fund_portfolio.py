import traceback
import mytusharepro
from Stock import mydb
from util import mydate
import pandas as pd
import re, time
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import sessionmaker
from Fund.model import FundPortfolio
import numpy as np

pro = mytusharepro.MyTusharePro()


def collect_fund_portfolio():
    now = time.strftime('%Y%m%d', time.localtime(time.time()))

    # 连接数据库
    conn = mydb.conn()
    cursor = conn.cursor()

    cursor.execute(
        "create table if not exists fund_portfolio ("
        "ts_code varchar(20),"
        "ann_date varchar(20),"
        "end_date varchar(20),"
        "symbol varchar(500),"
        "mkv float,"
        "amount float,"
        "stk_mkv_ratio float,"
        "stk_float_ratio float,"
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
    print("portfolio {0}".format(ts_code))
    df_portfolio = pro.fund_portfolio(ts_code=ts_code, fields='ts_code,ann_date,end_date,symbol,'
                                                              'mkv,amount,stk_mkv_ratio,stk_float_ratio')

    engine = mydb.engine()
    conn = engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()

    exist_rows = pd.read_sql("select * from fund_portfolio where ts_code='{0}'".format(ts_code), conn)
    if len(exist_rows) >= len(df_portfolio):
        return

    df_temp = exist_rows.append(df_portfolio)
    df_portfolio = df_temp.drop_duplicates(subset=['ts_code','end_date','symbol'], keep=False)

    for index, row in df_portfolio.iterrows():
        portfolio = FundPortfolio()
        portfolio.ts_code = row["ts_code"]
        portfolio.ann_date = row["ann_date"]
        portfolio.end_date = row["end_date"]
        portfolio.symbol = row["symbol"]
        portfolio.mkv = None if np.isnan(row["mkv"]) else row["mkv"]
        portfolio.amount = None if np.isnan(row["amount"]) else row["amount"]
        portfolio.stk_mkv_ratio = None if np.isnan(row["stk_mkv_ratio"]) else row["stk_mkv_ratio"]
        portfolio.stk_float_ratio = None if np.isnan(row["stk_float_ratio"]) else row["stk_float_ratio"]
        exist_rows = session.query(FundPortfolio).filter(and_(FundPortfolio.ts_code == portfolio.ts_code,
                                                                    FundPortfolio.end_date == portfolio.end_date,
                                                                    FundPortfolio.symbol == portfolio.symbol,
                                                                    )).all()
        if len(exist_rows) == 0:
            print("insert fund portfolio {0},{1},{2}".format(portfolio.ts_code,portfolio.end_date,portfolio.symbol))
            session.add(portfolio)
            session.commit()
        else:
            print("skipped {0},{1},{2}".format(portfolio.ts_code,portfolio.end_date,portfolio.symbol))

    conn.close()
    engine.dispose()


if __name__ == '__main__':
    collect_fund_portfolio()


