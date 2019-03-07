import tushare as ts
import time


class MyTusharePro:
    def __init__(self):
        self.pro = ts.pro_api("0577694ff6087849a141deb1c12ddf8566710906b8f64548f03183ce")
        self.max_call_times = 5

    def disclosure_date(self, ts_code=None, end_date=None, pre_date=None, actual_date=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.disclosure_date(ts_code=ts_code, end_date=end_date, pre_date=pre_date,
                                            actual_date=actual_date)
        except Exception as e:
            time.sleep(0.3)
            return self.disclosure_date(ts_code, end_date, pre_date, actual_date, time + 1)

    def income(self, ts_code=None, ann_date=None, start_date=None, end_date=None, period=None, report_type=None,
               comp_type=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.income(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                   period=period, report_type=report_type, comp_type=comp_type
                                   )
        except Exception as e:
            time.sleep(0.3)
            return self.income(ts_code, ann_date, start_date, end_date, period, report_type, comp_type, times + 1)

    def balancesheet(self, ts_code=None, ann_date=None, start_date=None, end_date=None, period=None, report_type=None,
                     comp_type=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.balancesheet(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                         period=period, report_type=report_type, comp_type=comp_type
                                         )
        except Exception as e:
            time.sleep(0.3)
            return self.balancesheet(ts_code, ann_date, start_date, end_date, period, report_type, comp_type, time + 1)

    def fina_indicator(self, ts_code=None, ann_date=None, start_date=None, end_date=None, period=None, times=1):
        if times > self.max_call_times:
            raise Exception("尝试调用Tushare超出最大次数!", 1)
        try:
            return self.pro.fina_indicator(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                                   period=period
                                   )
        except Exception as e:
            time.sleep(0.3)
            return self.fina_indicator(ts_code, ann_date, start_date, end_date, period, times + 1)