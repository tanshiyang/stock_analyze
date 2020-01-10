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
import my_email.sendmail as sm
import util.df_util as df_util
from concurrent.futures import ThreadPoolExecutor

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
    print(str.format("m{0}, query_top_{1}: {2}", m, top_n, trade_date))

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
        cursor.execute("select  max(trade_date) from %s where top_type='m%s'" % (tablename, m))
        last_date = cursor.fetchone()[0]

    if last_date is None:
        last_date = "20150101"

    last_date = mydate.string_to_next_day(last_date)
    today = time.strftime('%Y%m%d')
    with ThreadPoolExecutor(10) as executor:
        while last_date <= today:
            executor.submit(query_top_n, m, last_date, top_n)
            # query_top_n(m, last_date, top_n)
            last_date = mydate.string_to_next_day(last_date)

def send_result_mail():
    conn = mydb.conn()
    cursor = conn.cursor()
    today = time.strftime('%Y%m%d')
    html = ""
    sql_template = """
    SELECT * from rps_tops where top_type='m{0}' and trade_date='{1}' order by extrs desc 
    """
    for m in [50, 120, 250]:
        sql = sql_template.format(m, today)
        df = pd.read_sql(sql, conn)
        df = df_util.append_fina_indicator(df)
        if df.__len__() == 0:
            continue

        html += "{0}，RPS{1}排名：".format(today, m)
        html += "<table border='1'>"
        html += "<tr>"
        html += "<td>序号</td>"
        html += "<td>日期</td>"
        html += "<td>代码</td>"
        html += "<td>得分</td>"
        html += "<td>财报周期</td>"
        html += "<td>净利润增长率</td>"
        html += "<td>加权净资产收益率</td>"
        html += "<td>资产负债比率</td>"
        html += "<td>流动比率</td>"
        html += "<td>速动比率</td>"
        html += "</tr>"
        for index, row in df.iterrows():
            html += "<tr>"
            html += "<td>{0}</td>".format(index)
            html += "<td>{0}</td>".format(row["trade_date"])
            html += "<td>{0}</td>".format(row["ts_code"])
            html += "<td>{0}</td>".format(row["extrs"])
            html += "<td>{0}</td>".format(row["财报周期"])
            html += "<td>{0}</td>".format(row["净利润增长率"])
            html += "<td>{0}</td>".format(row["加权净资产收益率"])
            html += "<td>{0}</td>".format(row["资产负债比率"])
            html += "<td>{0}</td>".format(row["流动比率"])
            html += "<td>{0}</td>".format(row["速动比率"])
            html += "</tr>"
        html += "</table>"

    relative_day = mydate.string_to_relative_days(today, -5)
    sql_template = """
    SELECT ts_code,sum(extrs) 总分,count(0) 出现次数, MAX(trade_date) AS trade_date from rps_tops where top_type='m{0}' and trade_date>'{1}'
group by ts_code order by sum(extrs) desc;
    """
    for m in [50, 120, 250]:
        sql = sql_template.format(m, relative_day)
        df = pd.read_sql(sql, conn)
        df = df_util.append_fina_indicator(df)
        if df.__len__() == 0:
            continue

        html += "近5日RPS{0}排名：".format(m)
        html += "<table border='1'>"
        html += "<tr>"
        html += "<td>序号</td>"
        html += "<td>代码</td>"
        html += "<td>总分</td>"
        html += "<td>出现次数</td>"
        html += "<td>最近日期</td>"
        html += "<td>财报周期</td>"
        html += "<td>净利润增长率</td>"
        html += "<td>加权净资产收益率</td>"
        html += "<td>资产负债比率</td>"
        html += "<td>流动比率</td>"
        html += "<td>速动比率</td>"
        html += "</tr>"
        for index, row in df.iterrows():
            html += "<tr>"
            html += "<td>{0}</td>".format(index)
            html += "<td>{0}</td>".format(row["ts_code"])
            html += "<td>{0}</td>".format(row["总分"])
            html += "<td>{0}</td>".format(row["出现次数"])
            html += "<td>{0}</td>".format(row["trade_date"])
            html += "<td>{0}</td>".format(row["财报周期"])
            html += "<td>{0}</td>".format(row["净利润增长率"])
            html += "<td>{0}</td>".format(row["加权净资产收益率"])
            html += "<td>{0}</td>".format(row["资产负债比率"])
            html += "<td>{0}</td>".format(row["流动比率"])
            html += "<td>{0}</td>".format(row["速动比率"])
            html += "</tr>"
        html += "</table>"

    html += get_day_recommand(today)

    if html.__len__() > 0:
        sm.send_rps_mail(html)

def get_day_recommand(trade_date):
    conn = mydb.conn()
    cursor = conn.cursor()
    html = ""
    sql_template = """
        SELECT a.*,b.pe from (
        select min(trade_date) trade_date,min(ts_code) ts_code from (
        SELECT a.trade_date,ts_code,count(0) from rps_tops a 
        group by trade_date,ts_code
        HAVING count(0)>1 AND avg(extrs) >870 
        )a
        group BY a.ts_code, LEFT(a.trade_date,6)
        ) a join daily_basic b on a.trade_date=b.trade_date and a.ts_code=b.ts_code
        AND LEFT(a.trade_date,4)-LEFT((select MIN(trade_date) FROM daily_basic WHERE ts_code = a.ts_code),4)>2
        AND a.trade_date='{0}'
        order by trade_date;
        """
    sql = str.format(sql_template, trade_date)
    df = pd.read_sql(sql, conn)
    df = df_util.append_fina_indicator(df)
    if df.__len__() > 0:
        html += "今日优选："
        html += "<table border='1'>"
        html += "<tr>"
        html += "<td>序号</td>"
        html += "<td>日期</td>"
        html += "<td>代码</td>"
        html += "<td>pe</td>"
        html += "<td>财报周期</td>"
        html += "<td>净利润增长率</td>"
        html += "<td>加权净资产收益率</td>"
        html += "<td>资产负债比率</td>"
        html += "<td>流动比率</td>"
        html += "<td>速动比率</td>"
        html += "</tr>"
        for index, row in df.iterrows():
            html += "<tr>"
            html += "<td>{0}</td>".format(index)
            html += "<td>{0}</td>".format(row["trade_date"])
            html += "<td>{0}</td>".format(row["ts_code"])
            html += "<td>{0}</td>".format(row["pe"])
            html += "<td>{0}</td>".format(row["财报周期"])
            html += "<td>{0}</td>".format(row["净利润增长率"])
            html += "<td>{0}</td>".format(row["加权净资产收益率"])
            html += "<td>{0}</td>".format(row["资产负债比率"])
            html += "<td>{0}</td>".format(row["流动比率"])
            html += "<td>{0}</td>".format(row["速动比率"])
            html += "</tr>"
        html += "</table>"
        return  html

if __name__ == '__main__':
    # batch_query_top_n(50, '20000101', 20)
    # batch_query_top_n(120, '20000101', 20)
    # batch_query_top_n(250, '20000101', 20)
    print(get_day_recommand('20200109'))
