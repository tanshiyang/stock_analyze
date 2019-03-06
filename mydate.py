import datetime
import time
from dateutil.relativedelta import relativedelta


# 把datetime转成字符串
def date_to_string(dt):
    return dt.strftime("%Y%m%d")


# 把字符串转成datetime
def string_to_date(string):
    return datetime.datetime.strptime(string, "%Y%m%d")


# 把字符串的日期向后推3个月，返回字符型
def string_to_next_quarter(string):
    return date_to_string(string_to_date(string) + relativedelta(months=3))


def string_to_next_day(string):
    return date_to_string(string_to_date(string) + relativedelta(days=1))


def string_to_prev_day(string):
    return date_to_string(string_to_date(string) + relativedelta(days=-1))


def get_quarter():
    month = time.strftime('%m', time.localtime(time.time()))
    if '01' <= month <= '03':
        return 1
    if '04' <= month <= '06':
        return 2
    if '07' <= month <= '09':
        return 3
    if '10' <= month <= '12':
        return 4


def get_period_info():
    year = int(time.strftime('%Y', time.localtime(time.time())))
    month = time.strftime('%m', time.localtime(time.time()))
    day = time.strftime('%d', time.localtime(time.time()))
    quarter = get_quarter()
    if quarter == 2:
        return year + "0331", year + "01"
    if quarter == 3:
        return year + "0630", year + "02"
    if quarter == 4:
        return year + "0930", year + "03"
    if quarter == 1:
        return str(year-1)+ "1231", str(year-1) + "04"
