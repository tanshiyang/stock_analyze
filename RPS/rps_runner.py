import os
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from Stock import daily, daily_basic
import RPS.calc_rps as cr
import RPS.query_rps as qr
import MA.calc_ma as ma

if __name__ == '__main__':
    daily_basic.collect_daily_basic()
    daily.collect_daily_qfq()

    for m in [10, 20, 30, 60, 100, 250]:
        ma.calc_ma(m)
        
    for m in [50, 120, 250]:
        cr.calc_uprate(m)
        cr.batch_normalization(m)
        qr.batch_query_top_n(m)

    qr.send_result_mail()
