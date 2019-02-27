import datetime
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