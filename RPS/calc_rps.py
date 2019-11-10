import pandas as pd
import numpy as np
import copy
import mysql.connector
import re, time
import mydate
import mydb
import mytusharepro
import tradeday
from sqlalchemy import create_engine, Table, Column, Integer, String,Float, MetaData, ForeignKey

def calc_rps(last_date):
    conn = mydb.conn()
    cursor = conn.cursor()
    df = pd.read_sql("select * from daily where ts_code like '300300.sz%'",conn)
    df.x = copy.deepcopy(df.close)
