import urllib.request as request
import datetime
import mydate
import mytusharepro

'''
@query a single date: string '20170401';
@api return day_type: 0 workday 1 weekend 2 holiday -1 err
@function return day_type: 1 workday 0 weekend&holiday
'''


def get_day_type(query_date):
    url = 'http://tool.bitefu.net/jiari/?d=' + query_date
    resp = request.urlopen(url)
    content = resp.read()
    if content:
        try:
            day_type = int(str(content, encoding = "utf-8").replace("\"",""))
        except ValueError:
            return -1
        else:
            return day_type
    else:
        return -1


def is_tradeday(query_date):
    '''
    weekday = datetime.datetime.strptime(query_date, '%Y%m%d').isoweekday()
    if weekday <= 5 and get_day_type(query_date) == 0:
        return 1
    else:
        return 0
    '''
    pro = mytusharepro.MyTusharePro()
    cal = pro.trade_cal(start_date=query_date, end_date=query_date)
    if len(cal) == 0:
        return 0
    return cal.is_open[0]

def today_is_tradeday():
    query_date = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
    return is_tradeday(query_date)

def get_next_tradeday(date):
    while is_tradeday(date) == 0:
        date = mydate.string_to_next_day(date)
    return date


def get_prev_tradeday(date):
    while is_tradeday(date) == 0:
        date = mydate.string_to_prev_day(date)
    return date

if __name__ == '__main__':
    print(is_tradeday('20170406'))