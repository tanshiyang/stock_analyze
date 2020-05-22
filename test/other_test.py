import time
import pandas as pd
from Stock import mydb
import threading
import mytusharepro
from concurrent.futures import ThreadPoolExecutor
import util.df_util as df_util

pro = mytusharepro.MyTusharePro()

lock = threading.RLock()


def test():
    engine = mydb.engine()
    conn = mydb.conn()
    cursor = conn.cursor()

    sql = "SELECT * FROM ma_10 limit 100000"
    df = pd.read_sql(sql, conn)

    start_time = time.time()
    df_util.append_column(df, "new")
    with ThreadPoolExecutor(10) as executor:
        for index, row in df.iterrows():
            executor.submit(fun, df, index)
    end_time = time.time()
    print("用时：", end_time - start_time)
    conn.close()
    cursor.close()
    # for index, row in df.iterrows():
    #     print(df.loc[index, "ma"])


def fun(df, index):
    set_df(df, index)
    # df.loc[index, "new"] = 1000


def set_df(df, index):
    lock.acquire()
    try:
        df.loc[index, "new"] = 1000
    finally:
        lock.release()


if __name__ == '__main__':
    test()


