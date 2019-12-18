import mytusharepro
import tradeday
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, ForeignKey
from concurrent.futures import ThreadPoolExecutor

pro = mytusharepro.MyTusharePro()

if __name__ == '__main__':
    df = pro.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20180101', end_date='20181011')
    print('')
