import Stock.mydb as mydb
import pandas as pd
import datacompy
import time
import util.df_util as df_util
from util import mydate


def calc(period1, period2):
    conn = mydb.conn()
    year = int(time.strftime('%Y')) - 2
    sql = """
     SELECT  a.symbol,b.name,sum(mkv)
    from fund_portfolio a join stock_basic b on a.symbol=b.ts_code
    where a.ts_code in
    ( select * from (SELECT
        a.ts_code
        FROM
            fund_volatility a
            JOIN fund_basic b ON a.ts_code = b.ts_code 
        WHERE
            a.volatility > 0 
            AND b.found_date <= '{0}' 
            AND due_date IS NULL 
        ORDER BY
            a.volatility 
        limit 20) as tscodes
        )
    and end_date = '{1}'
    group by a.symbol  
    order by sum(mkv) desc
    -- limit 30
    """
    df1 = pd.read_sql(sql.format(year, period1), conn)
    df2 = pd.read_sql(sql.format(year, period2), conn)

    compare = datacompy.Compare(df1=df1, df2=df2, join_columns='symbol')
    # print(compare.report())

    pd.set_option('display.float_format', lambda x: format(x, ',.1f'))
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('max_colwidth', 100)

    print("\n{0},{1}:".format(period1, period2))
    print("\n交集:")
    df_util.append_column(compare.intersect_rows, 'test')
    compare.intersect_rows["diff"] = (compare.intersect_rows["sum(mkv)_df2"] - compare.intersect_rows[
        "sum(mkv)_df1"]) / compare.intersect_rows["sum(mkv)_df1"]
    compare.intersect_rows.drop(columns=['_merge', 'sum(mkv)_match','name_df2','name_match'], inplace=True)
    compare.intersect_rows = append_price(compare.intersect_rows, period1, 30)
    compare.intersect_rows = append_price(compare.intersect_rows, period1, 60)
    print(compare.intersect_rows)
    print("\n只在{0}中出现（转仓减仓）：".format(period1))
    print(compare.df1_unq_rows)
    print("\n只在{0}中出现（转仓加仓）：".format(period2))
    print(compare.df2_unq_rows)
    conn.close()


def append_price(df, period, relative_days):
    conn = mydb.conn()
    cursor = conn.cursor()
    column_name = '收盘价%s' % relative_days
    df_util.append_column(df, column_name)
    for index, row in df.iterrows():
        ts_code = row["symbol"]
        trade_date = period
        trade_date = mydate.string_to_relative_days(trade_date, relative_days)
        print("append_price,{0},{1}".format(ts_code, trade_date))
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
    conn.close()
    cursor.close()
    return df


if __name__ == '__main__':
    calc('20191231', '20200331')
