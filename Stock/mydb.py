import mysql.connector
from sqlalchemy import create_engine

conn_string = 'mysql://root:123123@localhost/mystock?charset=utf8'

def conn():
    return mysql.connector.connect(user='root', password='123123', database='mystock', auth_plugin='mysql_native_password')

def engine():
    return create_engine('mysql://root:123123@localhost/mystock?charset=utf8')

def fns_conn():
    return mysql.connector.connect(user='root', password='123123', database='fns', auth_plugin='mysql_native_password')

def fns_engine():
    return create_engine('mysql://root:123123@localhost/fns?charset=utf8')