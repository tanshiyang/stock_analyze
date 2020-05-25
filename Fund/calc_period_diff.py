import Stock.mydb as mydb
import pandas as pd
import datacompy
import time


def calc(period1, period2):
    conn = mydb.conn()
    year = int(time.strftime('%Y')) - 2
    sql = """
    SELECT  symbol,sum(mkv)
    from fund_portfolio where ts_code in
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
        limit 10) as tscodes
        )
    and end_date = '{1}'
    group by symbol  
    order by sum(mkv) desc
    limit 20
    """
    df1 = pd.read_sql(sql.format(year, period1), conn)
    df2 = pd.read_sql(sql.format(year, period2), conn)

    compare = datacompy.Compare(df1=df1, df2=df2, join_columns='symbol')
    print(compare.report())
    conn.close()


if __name__ == '__main__':
    calc('20191231','20200331')
