import mysql.connector


def conn():
    return mysql.connector.connect(user='root', password='123123', database='mystock')