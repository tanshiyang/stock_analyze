import os
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import Stock.mydb as mydb
import pandas as pd
import numpy as np
import datacompy
import time
import util.df_util as df_util
from util import mydate


def calc(period1, period2):
    conn = mydb.conn()
    sql = """
     SELECT  a.symbol,b.name,b.industry,sum(mkv) -- ,GROUP_CONCAT(CONCAT(' ',a.ts_code))
    from fund_portfolio a join stock_basic b on a.symbol=b.ts_code
    where 1=1
    and a.ts_code in
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
            AND b.fund_type in ('股票型')
            AND EXISTS(select 1 from fund_portfolio c where c.ts_code=a.ts_code)
        ORDER BY
            a.volatility 
        limit 30) as tscodes
        )
    and end_date = '{2}'
    group by a.symbol  
    order by sum(mkv) desc
    limit 60
    """
    from_year = int(period2[0:4]) - 1
    to_year = int(period2[0:4])
    df2 = pd.read_sql(sql.format(from_year, to_year, period2), conn)
    if len(df2) == 0:
        print(sql.format(from_year, to_year, period2))

    from_year = int(period1[0:4]) - 1
    to_year = int(period1[0:4])
    df1 = pd.read_sql(sql.format(from_year, to_year, period1), conn)
    if len(df1) == 0:
        print(sql.format(from_year, to_year, period1))

    compare = datacompy.Compare(df1=df2, df2=df1, join_columns='symbol')
    # print(compare.report())

    pd.set_option('display.float_format', lambda x: format(x, ',.1f'))
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('max_colwidth', 100)
    pd.set_option('display.width', 5000)

    df1 = append_price(df1, period2, 1)
    df1 = append_price(df1, period2, 60)

    df = df2
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
    return [df1, df2]


def append_price(df, period, relative_days):
    trade_date = get_period_ann_date(period, relative_days)
    column_name = '%s' % trade_date
    df_util.append_column(df, column_name)
    for index, row in df.iterrows():
        ts_code = row["symbol"]
        price = get_price(ts_code, trade_date)
        df.loc[index, column_name] = price
    return df


def get_price(ts_code, trade_date):
    conn = mydb.conn()
    cursor = conn.cursor()

    try:
        sql = str.format("show tables  like 'daily_{0}'", ts_code)
        cursor.execute(sql)
        table_exists = cursor.fetchall().__len__() > 0

        if not table_exists:
            return 0

        sql = str.format("select close from `daily_{0}` where trade_date>='{1}' limit 1", ts_code, trade_date)
        cursor.execute(sql)
        price_row = cursor.fetchone()
        if price_row is not None:
            return price_row[0]
        else:
            sql = str.format("select close from `daily_{0}` where trade_date<='{1}' order by trade_date desc limit 1",
                             ts_code, trade_date)
            cursor.execute(sql)
            price_row = cursor.fetchone()
            if price_row is not None:
                return price_row[0]
        return 0
    finally:
        conn.close()
        cursor.close()


def get_period_ann_date(period, relative_days):
    period = period.replace("0331", "0416")
    period = period.replace("0630", "0716")
    period = period.replace("0930", "1022")
    if "1231" in period:
        period = mydate.string_to_relative_years(period, 1)
        period = period.replace("1231", "0116")
    period = mydate.string_to_relative_days(period, relative_days)
    return period


def process_my_stocks(period1, period2, period1df, period2df, my_stocks_df=None):
    my_stock_max_counts = 2

    if my_stocks_df is None:
        my_stocks_df = pd.DataFrame(
            columns=["symbol", "name", "in_price", "in_date", "out_price", "out_date", "uprate"])

    compare = datacompy.Compare(df1=period2df, df2=period1df, join_columns='symbol')
    period1df_unq_rows = compare.df2_unq_rows

    filter_df = my_stocks_df[pd.isna(my_stocks_df["out_date"])]
    for index in filter_df.index:
        symbol = filter_df.at[index, 'symbol']
        still_exists_in_period2 = len(period2df[period2df.symbol == symbol]) > 0
        if not still_exists_in_period2:
            out_date = get_period_ann_date(period2, 1)
            my_stocks_df.at[index, 'out_date'] = out_date
            my_stocks_df.at[index, 'out_price'] = get_price(symbol, out_date)
            my_stocks_df.at[index, 'uprate'] = (my_stocks_df.at[index, 'out_price'] - my_stocks_df.at[
                index, 'in_price']) / my_stocks_df.at[index, 'out_price']

    while len(period2df) > 0 and len(my_stocks_df[pd.isna(my_stocks_df["out_date"])]) < my_stock_max_counts:
        for index, row in period2df.iterrows():
            criterion = my_stocks_df['symbol'].map(lambda x: x == row["symbol"])
            filter_df = my_stocks_df[criterion & pd.isna(my_stocks_df["out_date"])]
            if len(filter_df) == 0:
                in_date = get_period_ann_date(period2, 1)
                new = pd.DataFrame({'symbol': row["symbol"], 'name': row["name"],
                                    'in_price': row[in_date], 'in_date': in_date}, index=[0])
                my_stocks_df = my_stocks_df.append(new, ignore_index=True)
                break

    return my_stocks_df


if __name__ == '__main__':
    if len(sys.argv) == 3:
        calc(sys.argv[1], sys.argv[2])
    else:
        my_stocks_df = None
        now_year = int(time.strftime('%Y'))
        for year in range(2006, now_year):
            ret = calc('{0}0331'.format(year), '{0}0630'.format(year))
            my_stocks_df = process_my_stocks('{0}0331'.format(year), '{0}0630'.format(year), ret[0], ret[1],
                                             my_stocks_df)
            ret = calc('{0}0630'.format(year), '{0}0930'.format(year))
            my_stocks_df = process_my_stocks('{0}0630'.format(year), '{0}0930'.format(year), ret[0], ret[1],
                                             my_stocks_df)
            ret = calc('{0}0930'.format(year), '{0}1231'.format(year))
            my_stocks_df = process_my_stocks('{0}0930'.format(year), '{0}1231'.format(year), ret[0], ret[1],
                                             my_stocks_df)
            ret = calc('{0}1231'.format(year), '{0}0331'.format(year + 1))
            my_stocks_df = process_my_stocks('{0}1231'.format(year), '{0}0331'.format(year + 1), ret[0], ret[1],
                                             my_stocks_df)
        print(my_stocks_df.to_html())
