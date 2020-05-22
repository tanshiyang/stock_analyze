from Stock import mydb
from util.df_util import append_column
from MA.bull_arrange import get_rolling_bull_arrange

def append_fina_indicator(df):
    conn = mydb.conn()
    cursor = conn.cursor()
    append_column(df, '财报周期')
    append_column(df, '净利润增长率')
    append_column(df, '加权净资产收益率')
    append_column(df, '资产负债比率')
    append_column(df, '流动比率')
    append_column(df, '速动比率')
    for index, row in df.iterrows():
        ts_code = row["ts_code"]
        trade_date = row["trade_date"]

        print("appending fina indicator:{0},{1}".format(ts_code,trade_date))
        sql = str.format("SELECT end_date 财报周期, netprofit_yoy 净利润增长率, roe_waa AS 加权净资产收益率,debt_to_assets AS 资产负债比率, current_ratio AS "
                         "流动比率, quick_ratio AS 速动比率  FROM fina_indicator WHERE ts_code='{0}' AND end_date < "
                         "'{1}' ORDER BY end_date desc LIMIT 1; ", ts_code, trade_date)
        cursor.execute(sql)
        price_row = cursor.fetchone()
        if price_row is not None:
            df.loc[index, '财报周期'] = price_row[0]
            df.loc[index, '净利润增长率'] = price_row[1]
            df.loc[index, '加权净资产收益率'] = price_row[2]
            df.loc[index, '资产负债比率'] = price_row[3]
            df.loc[index, '流动比率'] = price_row[4]
            df.loc[index, '速动比率'] = price_row[5]
    conn.close()
    return df


def append_ma_bull_arrange(df):
    conn = mydb.conn()
    cursor = conn.cursor()
    append_column(df, '多头排列数')
    for index, row in df.iterrows():
        ts_code = row["ts_code"]
        trade_date = row["trade_date"]

        df.loc[index, '多头排列数'] = get_rolling_bull_arrange(ts_code,trade_date,25)
    conn.close()
    return df

