import daily
import RPS.calc_rps as cr
import RPS.query_rps as qr

if __name__ == '__main__':
    daily.collect_daily()

    for m in [50, 120, 250]:
        cr.calc_uprate(m)
        cr.batch_normalization(m)
        qr.batch_query_top_n(m)

    qr.send_result_mail()
