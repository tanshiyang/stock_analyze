import mysql.connector
from sqlalchemy import create_engine

def conn():
    return mysql.connector.connect(user='root', password='123123', database='mystock', auth_plugin='mysql_native_password')

def engine():
    return create_engine('mysql://root:123123@localhost/mystock?charset=utf8')