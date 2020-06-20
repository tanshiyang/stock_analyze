# 计算波动率
import traceback
import mytusharepro
from Stock import mydb
from util import mydate
import pandas as pd
import numpy as np
import re, time


def compute_volatility(data):
    returns = np.diff(data) / data[:-1]
    # 计算对数收益率
    logreturns = np.diff(np.log(data))
    # 股票波动率：是对价格变动的一种衡量。
    # 年股票波动率：对数收益率的标准差除以对数收益率的平均值，然后再除以252个工作日的倒数的平方根。
    uprate = (data[len(data) - 1] - data[0]) / data[0]
    if np.mean(logreturns * 100) == 0:
        return 0
    annualVolatility = np.std(logreturns * 100) / np.mean(logreturns * 100)
    quarter_days = 252 / 4  # len(data)
    annualVolatility = annualVolatility / np.sqrt(1 / quarter_days)
    # 去除涨幅
    # annualVolatility = annualVolatility / uprate
    return annualVolatility


def compute_volatility2(data):
    uprate = (data[len(data) - 1] - data[0]) / data[0]
    # 计算简单的收益率：相邻两天的差除以前一天的价格
    returns = np.diff(data) / data[:-1]
    # 对数收益率
    # 对所有收盘价取对数
    logClose = np.log(data)
    diffLogClose = np.diff(logClose)
    # 对数收益率的标准差
    std = np.std(diffLogClose)
    # 计算年波动率
    annual_volatility = std / np.mean(diffLogClose)
    annual_volatility = uprate * 100 / annual_volatility

    year_volatility = annual_volatility / np.sqrt(1 / 252)
    # 计算月波动率
    month_volatility = annual_volatility / np.sqrt(1 / 12)

    return year_volatility


def calc_volatility(ts_code, from_year, to_year):
    # 连接数据库
    conn = mydb.conn()
    cursor = conn.cursor()

    try:
        sql = "delete from fund_volatility where ts_code='{0}' and from_year='{1}' and to_year='{2}'"
        sql = sql.format(ts_code, from_year, to_year)
        cursor.execute(sql)

        sql = """
        SELECT a.ts_code,a.end_date, a.unit_nav from fund_nav as a 
        WHERE a.ts_code='{0}'
        AND a.end_date >='{1}0101'
        AND a.end_date <='{2}1231'
        ORDER BY a.end_date
        """
        sql = sql.format(ts_code, from_year, to_year)
        df_nav = pd.read_sql(sql, conn)
        if len(df_nav) == 0:
            return
        data = df_nav["unit_nav"]

        volatility = compute_volatility(data)

        sql = "insert into fund_volatility(ts_code, volatility,from_year,to_year) values('{0}',{1},{2},{3})"
        sql = sql.format(ts_code, volatility, from_year, to_year)
        cursor.execute(sql)
    except Exception as ex:
        print('{0},{1},{2}'.format(ts_code, from_year, to_year))
        print(ex)
    finally:
        conn.commit()
        conn.close()
        cursor.close()
    return volatility


def do_work(from_year, to_year):
    now = time.strftime('%Y%m%d', time.localtime(time.time()))

    # 连接数据库
    conn = mydb.conn()
    cursor = conn.cursor()

    sql = """
    SELECT b.ts_code from fund_basic  as b 
    where b.due_date is  null and b.fund_type in ('股票型','混合型')
    AND b.found_date <= '{0}1231' 
    AND (due_date IS NULL or due_date < '{0}1231')
    """.format(from_year)
    funds = pd.read_sql(sql, conn)
    today = time.strftime('%Y%m%d')
    for index, row in funds.iterrows():
        try:
            ts_code = row["ts_code"]
            calc_volatility(ts_code, from_year, to_year)
        except Exception as e:
            print(ts_code, from_year, to_year)
            print(e)

    conn.close()
    cursor.close()


def calc_recent_year(now_year=time.strftime('%Y')):
    from_year = str(int(now_year) - 1)
    do_work(from_year, now_year)

    from_year = str(int(now_year) - 2)
    do_work(from_year, now_year)


def test():
    conn = mydb.conn()
    cursor = conn.cursor()

    sql = """
            SELECT a.ts_code,a.end_date, a.unit_nav from fund_nav as a 
        WHERE a.ts_code='310318.OF'
        AND a.end_date >='20080101'
        AND a.end_date <='20091231'
        ORDER BY a.end_date
      """
    df = pd.read_sql(sql, conn)
    # print(df["unit_nav"])
    print(compute_volatility(df["unit_nav"]))
    print(xxx(df["unit_nav"]))

    sql = """
                SELECT a.ts_code,a.end_date, a.unit_nav from fund_nav as a 
            WHERE a.ts_code='340006.OF'
            AND a.end_date >='20080101'
            AND a.end_date <='20091231'
            ORDER BY a.end_date
          """
    df = pd.read_sql(sql, conn)
    # print(df["unit_nav"])
    print(compute_volatility(df["unit_nav"]))
    print(xxx(df["unit_nav"]))

    conn.close()
    cursor.close()





if __name__ == '__main__':
    # test()
    # # exit(0)
    # data = np.array([1, 2, 3, 4, 5, 6, 7, 8])
    # print(data)
    # print(compute_volatility(data))
    # print(xxx(data))
    # data = np.array([1, 2, 3, 4, 5, 6, 6, 8])
    # print(data)
    # print(compute_volatility(data))
    # print(xxx(data))
    # data = np.array([1, 2, 3, 4, 5, 6, 5, 8])
    # print(data)
    # print(compute_volatility(data))
    # print(xxx(data))
    # exit(0)
    # # calc_volatility('519772.OF', '2014', '2016')
    # calc_recent_year(2020)
    # exit(0)
    # do_work('2015', '2017')
    # do_work('2016', '2018')
    # do_work('2017', '2019')
    # do_work('2018', '2020')
    now_year = int(time.strftime('%Y'))
    for year in range(2000, now_year):
        calc_recent_year(year)
    calc_recent_year()
