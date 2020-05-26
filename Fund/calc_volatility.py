# 计算波动率
from numpy import *
import traceback
import mytusharepro
from Stock import mydb
from util import mydate
import pandas as pd
import re, time


def calc_volatility(ts_code):
    # 连接数据库
    conn = mydb.conn()
    cursor = conn.cursor()
    year = int(time.strftime('%Y')) - 2
    sql= """
    SELECT a.ts_code,a.end_date, a.unit_nav from fund_nav as a 
    WHERE a.ts_code='{0}'
    AND a.end_date <='{1}'
    ORDER BY a.end_date
    """
    sql = sql.format(ts_code, year)
    df_nav = pd.read_sql(sql, conn)
    if len(df_nav) == 0:
        return
    data = df_nav["unit_nav"]
    returns = diff(data) / data[:-1]
    # 计算对数收益率
    logreturns = diff(log(data))
    # 股票波动率：是对价格变动的一种衡量。
    # 年股票波动率：对数收益率的标准差除以对数收益率的平均值，然后再除以252个工作日的倒数的平方根。
    annualVolatility = std(logreturns) / mean(logreturns)
    periods = 252 / 4 #len(data)
    annualVolatility = annualVolatility / sqrt(1 / periods)

    sql = "delete from fund_volatility where ts_code='{0}'"
    sql = sql.format(ts_code)
    cursor.execute(sql)

    sql = "insert into fund_volatility(ts_code, volatility) values('{0}',{1})"
    sql = sql.format(ts_code, annualVolatility)
    cursor.execute(sql)
    conn.commit()
    conn.close()
    cursor.close()
    return annualVolatility


def do_work():
    now = time.strftime('%Y%m%d', time.localtime(time.time()))

    # 连接数据库
    conn = mydb.conn()
    cursor = conn.cursor()

    sql = """
    SELECT b.ts_code from fund_basic  as b 
    where b.due_date is  null and b.fund_type in ('股票型','混合型')
    AND b.found_date <= '2018' 
    """
    funds = pd.read_sql(sql, conn)
    today = time.strftime('%Y%m%d')
    for index, row in funds.iterrows():
        try:
            ts_code = row["ts_code"]
            calc_volatility(ts_code)
        except Exception as e:
            print(ts_code)
            print(e)

    conn.close()
    cursor.close()


if __name__ == '__main__':
    calc_volatility('540008.OF')
    do_work()

