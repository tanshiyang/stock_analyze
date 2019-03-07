import my_tushare_pro
import my_db
import pandas as pd
import mysql.connector
import re, time
import mydate
import tradeday

pro = my_tushare_pro.My_Tushare_Pro()


# 公告计划
def collect_disclosure(period):
    now = time.strftime('%Y%m%d', time.localtime(time.time()))
    disclosure_date = pro.disclosure_date(end_date='%s' % period)
    disclosure_date = disclosure_date[disclosure_date.actual_date < now]
    ts_codes = disclosure_date.ts_code
    ann_dates = disclosure_date.ann_date
    end_dates = disclosure_date.end_date
    pre_dates = disclosure_date.pre_date
    actual_dates = disclosure_date.actual_date
    modify_dates = disclosure_date.modify_date

    # 连接数据库
    conn = my_db.conn()
    cursor = conn.cursor()

    cursor.execute(
        "create table if not exists disclosure ("
        "ts_code	 varchar(32),"
        "ann_date  varchar(32),"
        "end_date  varchar(32),"
        "pre_date  varchar(32),"
        "actual_date  varchar(32),"
        "modify_date  varchar(32),"
        "unique(ts_code,ann_date))")

    # 通过for循环以及获取A股只数来遍历每一只股票
    for x in ts_codes.index:
        try:
            exist_stocks = pd.read_sql("select * from disclosure where ts_code='%s' and ann_date='%s'" %
                                       (ts_codes[x], ann_dates[x]), conn, index_col="ts_code")
            if len(exist_stocks) > 0:
                print('%s skipped' % ts_codes[x])
                continue

            # 匹配深圳股票（因为整个A股太多，所以我选择深圳股票做个筛选）
            if re.match('000', ts_codes[x]) or re.match('002', ts_codes[x]) or re.match('60', ts_codes[x]):
                sql = "insert into disclosure(ts_code,ann_date,end_date,pre_date,actual_date,modify_date) values(" \
                      "'%s','%s','%s','%s','%s','%s')" % (ts_codes[x], ann_dates[x], end_dates[x], pre_dates[x], \
                                                          actual_dates[x], modify_dates[x])
                print(sql)
                cursor.execute(sql)
                conn.commit()
        except Exception as e:
            print(e)
    conn.close()
    cursor.close()


# 利润表
def collect_income(period):
    now = time.strftime('%Y%m%d', time.localtime(time.time()))
    disclosure_date = pro.disclosure_date(end_date='%s' % period)
    disclosure_date = disclosure_date[disclosure_date.actual_date < now]
    ts_codes = disclosure_date.ts_code

    # 连接数据库
    conn = my_db.conn()
    cursor = conn.cursor()

    cursor.execute(
        "create table if not exists income ("
        "ts_code varchar(200),"
        "ann_date varchar(200),"
        "f_ann_date varchar(200),"
        "end_date varchar(200),"
        "report_type varchar(200),"
        "comp_type varchar(200),"
        "basic_eps float,"
        "diluted_eps float,"
        "total_revenue float,"
        "revenue float,"
        "int_income float,"
        "prem_earned float,"
        "comm_income float,"
        "n_commis_income float,"
        "n_oth_income float,"
        "n_oth_b_income float,"
        "prem_income float,"
        "out_prem float,"
        "une_prem_reser float,"
        "reins_income float,"
        "n_sec_tb_income float,"
        "n_sec_uw_income float,"
        "n_asset_mg_income float,"
        "oth_b_income float,"
        "fv_value_chg_gain float,"
        "invest_income float,"
        "ass_invest_income float,"
        "forex_gain float,"
        "total_cogs float,"
        "oper_cost float,"
        "int_exp float,"
        "comm_exp float,"
        "biz_tax_surchg float,"
        "sell_exp float,"
        "admin_exp float,"
        "fin_exp float,"
        "assets_impair_loss float,"
        "prem_refund float,"
        "compens_payout float,"
        "reser_insur_liab float,"
        "div_payt float,"
        "reins_exp float,"
        "oper_exp float,"
        "compens_payout_refu float,"
        "insur_reser_refu float,"
        "reins_cost_refund float,"
        "other_bus_cost float,"
        "operate_profit float,"
        "non_oper_income float,"
        "non_oper_exp float,"
        "nca_disploss float,"
        "total_profit float,"
        "income_tax float,"
        "n_income float,"
        "n_income_attr_p float,"
        "minority_gain float,"
        "oth_compr_income float,"
        "t_compr_income float,"
        "compr_inc_attr_p float,"
        "compr_inc_attr_m_s float,"
        "ebit float,"
        "ebitda float,"
        "insurance_exp float,"
        "undist_profit float,"
        "distable_profit float,"
        "unique(ts_code,ann_date))")

    # 通过for循环以及获取A股只数来遍历每一只股票
    for x in ts_codes.index:
        try:
            income = pro.income(ts_code=ts_codes[x], end_date=period)
            if income is None or len(income) == 0:
                continue

            exist_stocks = pd.read_sql("select * from income where ts_code='%s' and ann_date='%s'" %
                                       (income.ts_code[0], income.ann_date[0]), conn, index_col="ts_code")
            if len(exist_stocks) > 0:
                continue

            sql = ("insert into income("
                   "ts_code,"
                   "ann_date,"
                   "f_ann_date,"
                   "end_date,"
                   "report_type,"
                   "comp_type,"
                   "basic_eps,"
                   "diluted_eps,"
                   "total_revenue,"
                   "revenue,"
                   "int_income,"
                   "prem_earned,"
                   "comm_income,"
                   "n_commis_income,"
                   "n_oth_income,"
                   "n_oth_b_income,"
                   "prem_income,"
                   "out_prem,"
                   "une_prem_reser,"
                   "reins_income,"
                   "n_sec_tb_income,"
                   "n_sec_uw_income,"
                   "n_asset_mg_income,"
                   "oth_b_income,"
                   "fv_value_chg_gain,"
                   "invest_income,"
                   "ass_invest_income,"
                   "forex_gain,"
                   "total_cogs,"
                   "oper_cost,"
                   "int_exp,"
                   "comm_exp,"
                   "biz_tax_surchg,"
                   "sell_exp,"
                   "admin_exp,"
                   "fin_exp,"
                   "assets_impair_loss,"
                   "prem_refund,"
                   "compens_payout,"
                   "reser_insur_liab,"
                   "div_payt,"
                   "reins_exp,"
                   "oper_exp,"
                   "compens_payout_refu,"
                   "insur_reser_refu,"
                   "reins_cost_refund,"
                   "other_bus_cost,"
                   "operate_profit,"
                   "non_oper_income,"
                   "non_oper_exp,"
                   "nca_disploss,"
                   "total_profit,"
                   "income_tax,"
                   "n_income,"
                   "n_income_attr_p,"
                   "minority_gain,"
                   "oth_compr_income,"
                   "t_compr_income,"
                   "compr_inc_attr_p,"
                   "compr_inc_attr_m_s,"
                   "ebit,"
                   "ebitda,"
                   "insurance_exp,"
                   "undist_profit,"
                   "distable_profit"
                   ") values("
                   + "'%s'," % income.ts_code[0]
                   + "'%s'," % income.ann_date[0]
                   + "'%s'," % income.f_ann_date[0]
                   + "'%s'," % income.end_date[0]
                   + "'%s'," % income.report_type[0]
                   + "'%s'," % income.comp_type[0]
                   + "'%s'," % income.basic_eps[0]
                   + "'%s'," % income.diluted_eps[0]
                   + "'%s'," % income.total_revenue[0]
                   + "'%s'," % income.revenue[0]
                   + "'%s'," % income.int_income[0]
                   + "'%s'," % income.prem_earned[0]
                   + "'%s'," % income.comm_income[0]
                   + "'%s'," % income.n_commis_income[0]
                   + "'%s'," % income.n_oth_income[0]
                   + "'%s'," % income.n_oth_b_income[0]
                   + "'%s'," % income.prem_income[0]
                   + "'%s'," % income.out_prem[0]
                   + "'%s'," % income.une_prem_reser[0]
                   + "'%s'," % income.reins_income[0]
                   + "'%s'," % income.n_sec_tb_income[0]
                   + "'%s'," % income.n_sec_uw_income[0]
                   + "'%s'," % income.n_asset_mg_income[0]
                   + "'%s'," % income.oth_b_income[0]
                   + "'%s'," % income.fv_value_chg_gain[0]
                   + "'%s'," % income.invest_income[0]
                   + "'%s'," % income.ass_invest_income[0]
                   + "'%s'," % income.forex_gain[0]
                   + "'%s'," % income.total_cogs[0]
                   + "'%s'," % income.oper_cost[0]
                   + "'%s'," % income.int_exp[0]
                   + "'%s'," % income.comm_exp[0]
                   + "'%s'," % income.biz_tax_surchg[0]
                   + "'%s'," % income.sell_exp[0]
                   + "'%s'," % income.admin_exp[0]
                   + "'%s'," % income.fin_exp[0]
                   + "'%s'," % income.assets_impair_loss[0]
                   + "'%s'," % income.prem_refund[0]
                   + "'%s'," % income.compens_payout[0]
                   + "'%s'," % income.reser_insur_liab[0]
                   + "'%s'," % income.div_payt[0]
                   + "'%s'," % income.reins_exp[0]
                   + "'%s'," % income.oper_exp[0]
                   + "'%s'," % income.compens_payout_refu[0]
                   + "'%s'," % income.insur_reser_refu[0]
                   + "'%s'," % income.reins_cost_refund[0]
                   + "'%s'," % income.other_bus_cost[0]
                   + "'%s'," % income.operate_profit[0]
                   + "'%s'," % income.non_oper_income[0]
                   + "'%s'," % income.non_oper_exp[0]
                   + "'%s'," % income.nca_disploss[0]
                   + "'%s'," % income.total_profit[0]
                   + "'%s'," % income.income_tax[0]
                   + "'%s'," % income.n_income[0]
                   + "'%s'," % income.n_income_attr_p[0]
                   + "'%s'," % income.minority_gain[0]
                   + "'%s'," % income.oth_compr_income[0]
                   + "'%s'," % income.t_compr_income[0]
                   + "'%s'," % income.compr_inc_attr_p[0]
                   + "'%s'," % income.compr_inc_attr_m_s[0]
                   + "'%s'," % income.ebit[0]
                   + "'%s'," % income.ebitda[0]
                   + "'%s'," % income.insurance_exp[0]
                   + "'%s'," % income.undist_profit[0]
                   + "'%s'" % income.distable_profit[0]
                   + ")")
            sql = sql.replace("'None'", "NULL").replace("'nan'", "NULL")
            print(sql)
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print(e)
    conn.close()
    cursor.close()


if __name__ == '__main__':
    period_info = mydate.get_period_info()
    period_date = period_info[0]
    # collect_disclosure(period_date)
    collect_income(period_date)
