import tushare as ts
import mysql.connector
import re, time
import mydate
import tradeday

pro = pro = ts.pro_api("0577694ff6087849a141deb1c12ddf8566710906b8f64548f03183ce")


def every_date(period, table_name):
    # 获取所有有股票
    # stock_info = ts.get_stock_basics()
    # codes = stock_info.index
    now = time.strftime('%Y%m%d', time.localtime(time.time()))
    disclosure_date = pro.disclosure_date(end_date='%s' % period)
    disclosure_date = disclosure_date[disclosure_date.actual_date < now]
    codes = disclosure_date.ts_code
    # 连接数据库
    conn = mysql.connector.connect(user='root', password='123123', database='mystock')
    cursor = conn.cursor()

    cursor.execute("drop table if exists %s;" % table_name)
    cursor.execute(
        "create table  %s "
        "(ts_code varchar(32),"
        "start_date varchar(32),"
        "end_date varchar(32),"
        "close1 float, close2 float,"
        "total_revenue float, "
        "accounts_receiv	float, "    # 应收帐款 
        "n_income	float, "            # 利润
        "adv_receipts	float, "        # 预收帐款
        "roe float,"
        "grossprofit_margin float,"
        "pe float,"
        "unique(ts_code))" % table_name)

    # 通过for循环以及获取A股只数来遍历每一只股票
    for x in range(0, len(codes)):
        try:
            # 匹配深圳股票（因为整个A股太多，所以我选择深圳股票做个筛选）
            if re.match('000', codes[x]) or re.match('002', codes[x]) or re.match('60', codes[x]):
                times = 0
                sql = ""
                while times <= 5 and sql == "":
                    times = times + 1
                    sql = one_stock(codes[x], period, times, table_name)

                if sql == "" or sql.__contains__("None"):
                    print('%s的数据异常' % codes[x])
                else:
                    print(sql)
                    cursor.execute(sql)
                    conn.commit()
        except Exception as e:
            print(e)
    conn.close()
    cursor.close()


def one_stock(code, period, times, table_name):
    if times > 5:
        return ""
    try:
        market_code = ""
        if re.match('000', code) or re.match('002', code):
            market_code = ".SZ"
        elif re.match('60', code):
            market_code = ".SH"
        if not("." in code):
            ts_code = code +market_code
        else:
            ts_code = code

        balancesheet = pro.balancesheet(ts_code=ts_code, end_date=period)
        adv_receipts = balancesheet.adv_receipts[0]  # 如取最早：len(balancesheet) - 1
        accounts_receiv = balancesheet.accounts_receiv[0]
        report_date = balancesheet.end_date[0]
        start_date = get_prev_tradeday(report_date)
        end_date = mydate.string_to_next_quarter(start_date)
        end_date = get_prev_tradeday(end_date)
        print("start:%s,end:%s" % (start_date, end_date))

        fina_indicator = pro.fina_indicator(ts_code=ts_code, end_date=period)
        roe = fina_indicator.roe[0]                     # 净资产收益率
        grossprofit_margin = fina_indicator.grossprofit_margin[0]   # 毛利率

        income = pro.income(ts_code=ts_code, end_date=period)
        total_revenue = income.total_revenue[0]  # 如取最早：len(income) - 1
        n_income =  income.n_income[0]
        print("total_revenue:%s" % total_revenue)

        daily = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        close1 = daily.close[len(daily) - 1]
        close2 = daily.close[0]
        end_date = daily.trade_date[0]          # 以最后交易日为准
        print("close1:%s,close2:%s" % (close1, close2))

        daily_basic = pro.daily_basic(ts_code=ts_code, trade_date=start_date)
        pe = daily_basic.pe[0]
        print("pe:%s" % pe)

        sql = "insert into %s (ts_code,start_date,end_date,close1, close2,total_revenue, n_income, adv_receipts," \
              "accounts_receiv,roe,grossprofit_margin,pe) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s') " % (
                  table_name, ts_code, start_date, end_date, close1, close2, total_revenue, n_income, adv_receipts,accounts_receiv,roe,grossprofit_margin, pe)

        # print(sql)
        return sql
    except Exception as e:
        time.sleep(0.6)
        print(e)
        return ""
    finally:
        time.sleep(0.3)

def get_next_tradeday(date):
    while(tradeday.is_tradeday(date)==0):
        date = mydate.string_to_next_day(date)
    return date


def get_prev_tradeday(date):
    while(tradeday.is_tradeday(date)==0):
        date = mydate.string_to_prev_day(date)
    return date


# every_date('20180701', '20181001','stock_analyze_1803')
every_date('20181231','stock_analyze_1804')

