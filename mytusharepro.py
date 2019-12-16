import tushare as ts
import time


class MyTusharePro:
    def __init__(self):
        ts.set_token("0577694ff6087849a141deb1c12ddf8566710906b8f64548f03183ce")
        self.pro = ts.pro_api("0577694ff6087849a141deb1c12ddf8566710906b8f64548f03183ce")
        self.max_call_times = 999999

    def disclosure_date(self, ts_code=None, end_date=None, pre_date=None, actual_date=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.disclosure_date(ts_code=ts_code, end_date=end_date, pre_date=pre_date,
                                            actual_date=actual_date)
        except Exception as e:
            print(e)
            print("休息 30s ")
            time.sleep(30)
            return self.disclosure_date(ts_code, end_date, pre_date, actual_date, times + 1)
        finally:
            time.sleep(0.3)

    def income(self, ts_code=None, ann_date=None, start_date=None, end_date=None, period=None, report_type=None,
               comp_type=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.income(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                   period=period, report_type=report_type, comp_type=comp_type
                                   )
        except Exception as e:
            print(e)
            print("休息 30s ")
            time.sleep(30)
            return self.income(ts_code, ann_date, start_date, end_date, period, report_type, comp_type, times + 1)
        finally:
            time.sleep(0.3)

    def balancesheet(self, ts_code=None, ann_date=None, start_date=None, end_date=None, period=None, report_type=None,
                     comp_type=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.balancesheet(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                         period=period, report_type=report_type, comp_type=comp_type
                                         )
        except Exception as e:
            print(e)
            print("休息 30s ")
            time.sleep(30)
            return self.balancesheet(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                     period=period, report_type=report_type, comp_type=comp_type, times=times + 1)
        finally:
            time.sleep(0.3)

    def fina_indicator(self, ts_code=None, ann_date=None, start_date=None, end_date=None, period=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.fina_indicator(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                           period=period
                                           )
        except Exception as e:
            print(e)
            print("休息 30s ")
            time.sleep(30)
            return self.fina_indicator(ts_code, ann_date, start_date, end_date, period, times + 1)
        finally:
            time.sleep(0.3)


    def daily(self, ts_code=None, trade_date=None, start_date=None, end_date=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.daily(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        except Exception as e:
            print(e)
            print("休息 30s ")
            time.sleep(30)
            return self.daily(ts_code, trade_date, start_date, end_date, times + 1)
        finally:
            time.sleep(0.3)


    def daily_basic(self, ts_code=None, trade_date=None, start_date=None, end_date=None, fields=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.daily_basic(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date, fields=fields)
        except Exception as e:
            print(e)
            print("休息 30s ")
            time.sleep(30)
            return self.daily_basic(ts_code, trade_date, start_date, end_date, fields, times + 1)
        finally:
            time.sleep(0.3)


    def trade_cal(self, exchange=None, start_date=None, end_date=None, is_open=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date, is_open=is_open)
        except Exception as e:
            print(e)
            print("休息 30s ")
            time.sleep(30)
            return self.trade_cal(exchange, start_date, end_date, is_open, times + 1)
        finally:
            time.sleep(0.3)

    def pro_bar(self, ts_code=None, start_date=None,end_date=None,adj=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return ts.pro_bar(ts_code=ts_code,start_date=start_date,end_date=end_date,adj=adj)
        except Exception as e:
            print(e)
            print("休息 30s ")
            time.sleep(30)
            return self.pro_bar(ts_code=ts_code,start_date=start_date,end_date=end_date,adj=adj, times=times + 1)
        finally:
            time.sleep(0.3)

    def stock_basic(self, is_hs=None, list_status=None,exchange=None, fields=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.stock_basic(is_hs=is_hs,list_status=list_status,exchange=exchange,fields=fields)
        except Exception as e:
            print(e)
            print("休息 60s ")
            time.sleep(60)
            return self.stock_basic(is_hs=is_hs,list_status=list_status,exchange=exchange, fields=fields,times=times + 1)
        finally:
            time.sleep(0.3)
