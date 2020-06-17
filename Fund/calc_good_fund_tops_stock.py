import os
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import Stock.mydb as mydb
import pandas as pd
import datacompy
import time
import util.df_util as df_util
from util import mydate


def calc(period1, period2):
    conn = mydb.conn()
    sql = """
     SELECT  a.symbol,b.name,b.industry,sum(mkv) -- ,GROUP_CONCAT(CONCAT(' ',a.ts_code))
    from fund_portfolio a join stock_basic b on a.symbol=b.ts_code
    where a.ts_code in
    ( select * from (SELECT
        a.ts_code
        FROM
            fund_volatility a
            JOIN fund_basic b ON a.ts_code = b.ts_code 
        WHERE
            a.volatility > 0 
            AND a.from_year = {0}
            AND a.to_year = {1}
            AND b.found_date <= '{0}' 
            AND due_date IS NULL 
            AND b.fund_type ='股票型'
        ORDER BY
            a.volatility 
        limit 60) as tscodes
        )
    and end_date = '{2}'
    group by a.symbol  
    order by sum(mkv) desc
    limit 20
    """
    from_year = int(period2[0:4]) - 2
    to_year = int(period2[0:4])
    df1 = pd.read_sql(sql.format(from_year, to_year, period2), conn)
    from_year = int(period1[0:4]) - 2
    to_year = int(period1[0:4])
    df2 = pd.read_sql(sql.format(from_year, to_year, period1), conn)

    compare = datacompy.Compare(df1=df1, df2=df2, join_columns='symbol')
    # print(compare.report())

    pd.set_option('display.float_format', lambda x: format(x, ',.1f'))
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('max_colwidth', 100)
    pd.set_option('display.width', 5000)

    df = df1
    if len(df) > 0:
        df = append_price(df, period2, 1)
        df = append_price(df, period2, 60)
        df["uprate"] = (df[get_period_ann_date(period2, 60)] - df[get_period_ann_date(period2, 1)]) / df[
            get_period_ann_date(period2, 1)]
        print("<p/>{0},{1}:".format(period1, period2))
        print("<p/>{0} Top 10:".format(period2))
        print(df.to_html())

    df = compare.intersect_rows
    if len(df) > 0:
        df_util.append_column(df, 'test')
        df["diff"] = (df["sum(mkv)_df1"] - df["sum(mkv)_df2"]) / df["sum(mkv)_df1"]
        df.drop(columns=['_merge', 'sum(mkv)_match', 'name_df2', 'name_match'], inplace=True)
        df.drop(columns=['industry_df2', 'industry_match'], inplace=True)
        df = append_price(df, period2, 1)
        df = append_price(df, period2, 60)
        df["uprate"] = (df[get_period_ann_date(period2, 60)] - df[get_period_ann_date(period2, 1)]) / df[
            get_period_ann_date(period2, 1)]
        print("<p/>交集:")
        print(df.to_html())

    df = compare.df1_unq_rows
    if len(df) > 0:
        df = append_price(df, period2, 1)
        df = append_price(df, period2, 60)
        df["uprate"] = (df[get_period_ann_date(period2, 60)] - df[get_period_ann_date(period2, 1)]) / df[
            get_period_ann_date(period2, 1)]
        print("<p/>只在{0}中出现（转仓加仓）：".format(period2))
        print(df.to_html())

    df = compare.df2_unq_rows
    if len(df) > 0:
        df = append_price(df, period2, 1)
        df = append_price(df, period2, 60)
        df["uprate"] = (df[get_period_ann_date(period2, 60)] - df[get_period_ann_date(period2, 1)]) / df[
            get_period_ann_date(period2, 1)]
        print("<p/>只在{0}中出现（转仓减仓）：".format(period1))
        print(df.to_html())
    conn.close()


def append_price(df, period, relative_days):
    conn = mydb.conn()
    cursor = conn.cursor()

    trade_date = get_period_ann_date(period, relative_days)

    column_name = '%s' % trade_date
    df_util.append_column(df, column_name)
    for index, row in df.iterrows():
        ts_code = row["symbol"]
        # print("append_price,{0},{1}".format(ts_code, trade_date))
        sql = str.format("show tables  like 'daily_{0}'", ts_code)
        cursor.execute(sql)
        table_exists = cursor.fetchall().__len__() > 0

        if not table_exists:
            continue

        sql = str.format("select close from `daily_{0}` where trade_date>='{1}' limit 1", ts_code, trade_date)
        cursor.execute(sql)
        price_row = cursor.fetchone()
        if price_row is not None:
            df.loc[index, column_name] = price_row[0]
        else:
            sql = str.format("select close from `daily_{0}` where trade_date<='{1}' order by trade_date desc limit 1",
                             ts_code, trade_date)
            cursor.execute(sql)
            price_row = cursor.fetchone()
            if price_row is not None:
                df.loc[index, column_name] = price_row[0]
    conn.close()
    cursor.close()
    return df


def get_period_ann_date(period, relative_days):
    period = period.replace("0331", "0416")
    period = period.replace("0630", "0716")
    period = period.replace("0930", "1022")
    if "1231" in period:
        period = mydate.string_to_relative_years(period, 1)
        period = period.replace("1231", "0116")
    period = mydate.string_to_relative_days(period, relative_days)
    return period


if __name__ == '__main__':
    if len(sys.argv) == 3:
        calc(sys.argv[1], sys.argv[2])
    else:
        now_year = int(time.strftime('%Y'))
        for year in range(2006, now_year):
            calc('{0}0331'.format(year), '{0}0630'.format(year))
            calc('{0}0630'.format(year), '{0}0930'.format(year))
            calc('{0}0930'.format(year), '{0}1231'.format(year))
            calc('{0}1231'.format(year), '{0}0331'.format(year + 1))
