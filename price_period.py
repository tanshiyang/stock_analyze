import pandas as pd
import mysql.connector
import re, time
import mydate
import mydb
import mytusharepro
import tradeday

pro = mytusharepro.MyTusharePro()


def collect_price(period, deleteolddata):
    table_name = "price_period"
    # 获取所有有股票
    # stock_info = ts.get_stock_basics()
    # codes = stock_info.index
    now = time.strftime('%Y%m%d', time.localtime(time.time()))
    disclosure_date = pro.disclosure_date(end_date='%s' % period)
    disclosure_date = disclosure_date[disclosure_date.actual_date < now]
    ts_codes = disclosure_date.ts_code
    # 连接数据库
    conn = mydb.conn()
    cursor = conn.cursor()

    cursor.execute(
        "create table if not exists %s "
        "(ts_code varchar(32),"
        "end_date varchar(32),"
        "start_trade_date varchar(32),"
        "close1 float, "
        "close_min float,"
        "close_min_date varchar(32),"
        "close_max float,"
        "close_max_date varchar(32),"
        "close2 float,"
        "end_trade_date varchar(32),"
        "pe float,"
        "unique(ts_code,end_date))" % table_name)

    # 通过for循环以及获取A股只数来遍历每一只股票
    for x in ts_codes.index:
        try:
            exist_stocks = pd.read_sql("select * from %s where ts_code='%s' and end_date='%s'"
                                       % (table_name, ts_codes[x], period), conn, index_col="ts_code")
            if len(exist_stocks) > 0:
                if deleteolddata is True:
                    sql = "delete from %s where ts_code='%s' and end_date='%s'" % (table_name, ts_codes[x], period)
                    cursor.execute(sql)
                else:
                    print('%s price_period skipped' % ts_codes[x])
                    continue

            # 匹配深圳股票（因为整个A股太多，所以我选择深圳股票做个筛选）
            if re.match('000', ts_codes[x]) or re.match('002', ts_codes[x]) or re.match('60', ts_codes[x]):
                times = 0
                sql = ""
                sql = one_stock(ts_codes[x], period, table_name)

                if sql == "" or sql.__contains__("None"):
                    print('%s 的数据异常' % ts_codes[x])
                else:
                    print(sql)
                    cursor.execute(sql)
                    conn.commit()
        except Exception as e:
            print(e)
    conn.close()
    cursor.close()


def one_stock(code, period, table_name):
    try:
        market_code = ""
        if re.match('000', code) or re.match('002', code):
            market_code = ".SZ"
        elif re.match('60', code):
            market_code = ".SH"
        if not ("." in code):
            ts_code = code + market_code
        else:
            ts_code = code

        balancesheet = pro.balancesheet(ts_code=ts_code)
        balancesheet = balancesheet.loc[balancesheet["end_date"] == period]
        report_date = balancesheet.ann_date.values[len(balancesheet)-1]
        start_trade_date = get_next_tradeday(report_date)
        next_quarter_date = mydate.string_to_next_quarter(start_trade_date)
        end_trade_date = get_prev_tradeday(next_quarter_date)
        print("start:%s,end:%s" % (start_trade_date, end_trade_date))

        daily = pro.daily(ts_code=ts_code, start_date=start_trade_date, end_date=end_trade_date)
        close1 = daily.close[len(daily) - 1]
        max_close_idx = daily.close.idxmax()
        min_close_idx = daily.close.idxmin()
        close_max = daily.close[max_close_idx]
        close_max_date = daily.trade_date[max_close_idx]
        close_min = daily.close[min_close_idx]
        close_min_date = daily.trade_date[min_close_idx]

        close2 = daily.close.values[0]
        end_trade_date = daily.trade_date.values[0]
        print("close1:%s,close2:%s" % (close1, close2))

        daily_basic = pro.daily_basic(ts_code=ts_code, trade_date=start_trade_date)
        pe = daily_basic.pe.values[0]
        print("pe:%s" % pe)

        sql = ("insert into %s (" % table_name
               + "ts_code,end_date,start_trade_date,close1,close_min,close_min_date,close_max,close_max_date,close2,end_trade_date,pe"
               ")values("
               + "'%s'," % ts_code
               + "'%s'," % period
               + "'%s'," % start_trade_date
               + "'%s'," % close1
               + "'%s'," % close_min
               + "'%s'," % close_min_date
               + "'%s'," % close_max
               + "'%s'," % close_max_date
               + "'%s'," % close2
               + "'%s'," % end_trade_date
               + "'%s'" % pe
               + ")")

        return sql
    except Exception as e:
        print(e)
        return ""

def get_next_tradeday(date):
    while tradeday.is_tradeday(date) == 0:
        date = mydate.string_to_next_day(date)
    return date


def get_prev_tradeday(date):
    while tradeday.is_tradeday(date) == 0:
        date = mydate.string_to_prev_day(date)
    return date


if __name__ == '__main__':
    # 20180331 20180630 20180930 20181231
    '''
    every_date('20180331', 'stock_analyze_1801', True)
    every_date('20180630', 'stock_analyze_1802', True)
    every_date('20181231', 'stock_analyze_1804', True)
    every_date('20170331', 'stock_analyze_1701', True)
    every_date('20170630', 'stock_analyze_1702', True)
    every_date('20170930', 'stock_analyze_1703', True)
    every_date('20171231', 'stock_analyze_1704', True)
    every_date('20190331', 'stock_analyze_1901', True)
    '''
    collect_price("20190331", False)
    for year in range(2017, 2019):
        for md in ["0331", "0630", "0930", "1231"]:
            period_date = str(year) + md
            collect_price(period_date, False)



