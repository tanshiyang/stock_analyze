import Stock.mydb as mydb
import pandas as pd
import datacompy
import time
import util.df_util as df_util

def calc(period1, period2):
    conn = mydb.conn()
    year = int(time.strftime('%Y')) - 2
    sql = """
     SELECT  concat(a.symbol,b.name) as symbol,sum(mkv)
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
            AND issue_date IS NULL 
        ORDER BY
            a.volatility 
        limit 20) as tscodes
        )
    and end_date = '{1}'
    group by a.symbol  
    order by sum(mkv) desc
    limit 20
    """
    df1 = pd.read_sql(sql.format(year, period1), conn)
    df2 = pd.read_sql(sql.format(year, period2), conn)

    compare = datacompy.Compare(df1=df1, df2=df2, join_columns='symbol')
    # print(compare.report())
    pd.set_option('display.float_format', lambda x: format(x, ',.1f'))
    print("\n{0},{1}:".format(period1, period2))
    print("\n交集:")
    df_util.append_column(compare.intersect_rows, 'test')
    compare.intersect_rows["diff"] = (compare.intersect_rows["sum(mkv)_df2"] - compare.intersect_rows[
        "sum(mkv)_df1"]) / compare.intersect_rows["sum(mkv)_df1"]
    compare.intersect_rows.drop(columns=['_merge', 'sum(mkv)_match'], inplace=True)
    print(compare.intersect_rows)
    print("\n只在{0}中出现（转仓减仓）：".format(period1))
    print(compare.df1_unq_rows)
    print("\n只在{0}中出现（转仓加仓）：".format(period2))
    print(compare.df2_unq_rows)
    conn.close()


if __name__ == '__main__':
    calc('20190930','20191231')
