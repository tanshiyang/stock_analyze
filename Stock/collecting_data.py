import traceback
import mytusharepro
from Stock import mydb, daily
from util import mydate
import pandas as pd
import re, time
from concurrent.futures import ThreadPoolExecutor

pro = mytusharepro.MyTusharePro()


# 公告计划
def collect_disclosure(period):
    now = time.strftime('%Y%m%d', time.localtime(time.time()))

    # 连接数据库
    conn = mydb.conn()
    cursor = conn.cursor()

    cursor.execute(
        "create table if not exists disclosure ("
        "ts_code	 varchar(32),"
        "ann_date  varchar(32),"
        "end_date  varchar(32),"
        "pre_date  varchar(32),"
        "actual_date  varchar(32),"
        "modify_date  varchar(32),"
        "unique(ts_code,end_date))")

    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
    today = time.strftime('%Y%m%d')
    for index, row in stocks.iterrows():
        try:
            ts_code = row["ts_code"]
            exist_stocks = pd.read_sql("select * from disclosure where ts_code='%s' and end_date='%s'" %
                                       (ts_code, period), conn, index_col="ts_code")
            if len(exist_stocks) > 0:
                print('%s %s disclosure skipped' % (ts_code, period))
                continue

            disclosure_date = pro.disclosure_date(end_date=period, ts_code=ts_code)
            disclosure_date = disclosure_date[disclosure_date.actual_date < now]

            if len(disclosure_date) == 0:
                continue
            ann_date = disclosure_date.iloc[0].ann_date
            end_date = disclosure_date.iloc[0].end_date
            pre_date = disclosure_date.iloc[0].pre_date
            actual_date = disclosure_date.iloc[0].actual_date
            # modify_dates = disclosure_date.modify_date

            # 匹配深圳股票（因为整个A股太多，所以我选择深圳股票做个筛选）
            if re.match('000', ts_code) or re.match('002', ts_code) or re.match('60', ts_code):
                sql = "insert into disclosure(ts_code,ann_date,end_date,pre_date,actual_date) values('%s','%s','%s'," \
                      "'%s','%s')" % (ts_code, ann_date, end_date, pre_date, actual_date)
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

    # 连接数据库
    conn = mydb.conn()
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

    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
    today = time.strftime('%Y%m%d')
    for index, row in stocks.iterrows():
        try:
            ts_code = row["ts_code"]
            exist_stocks = pd.read_sql("select * from income where ts_code='%s' and end_date='%s'" %
                                       (ts_code, period), conn, index_col="ts_code")
            if len(exist_stocks) > 0:
                print("%s %s income skipped." % (ts_code, period))
                continue

            income = pro.income(ts_code=ts_code)
            income = income.loc[income["end_date"] == period]

            if income is None or len(income) == 0:
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
                   + "'%s'," % income.ts_code.values[0]
                   + "'%s'," % income.ann_date.values[0]
                   + "'%s'," % income.f_ann_date.values[0]
                   + "'%s'," % income.end_date.values[0]
                   + "'%s'," % income.report_type.values[0]
                   + "'%s'," % income.comp_type.values[0]
                   + "'%s'," % income.basic_eps.values[0]
                   + "'%s'," % income.diluted_eps.values[0]
                   + "'%s'," % income.total_revenue.values[0]
                   + "'%s'," % income.revenue.values[0]
                   + "'%s'," % income.int_income.values[0]
                   + "'%s'," % income.prem_earned.values[0]
                   + "'%s'," % income.comm_income.values[0]
                   + "'%s'," % income.n_commis_income.values[0]
                   + "'%s'," % income.n_oth_income.values[0]
                   + "'%s'," % income.n_oth_b_income.values[0]
                   + "'%s'," % income.prem_income.values[0]
                   + "'%s'," % income.out_prem.values[0]
                   + "'%s'," % income.une_prem_reser.values[0]
                   + "'%s'," % income.reins_income.values[0]
                   + "'%s'," % income.n_sec_tb_income.values[0]
                   + "'%s'," % income.n_sec_uw_income.values[0]
                   + "'%s'," % income.n_asset_mg_income.values[0]
                   + "'%s'," % income.oth_b_income.values[0]
                   + "'%s'," % income.fv_value_chg_gain.values[0]
                   + "'%s'," % income.invest_income.values[0]
                   + "'%s'," % income.ass_invest_income.values[0]
                   + "'%s'," % income.forex_gain.values[0]
                   + "'%s'," % income.total_cogs.values[0]
                   + "'%s'," % income.oper_cost.values[0]
                   + "'%s'," % income.int_exp.values[0]
                   + "'%s'," % income.comm_exp.values[0]
                   + "'%s'," % income.biz_tax_surchg.values[0]
                   + "'%s'," % income.sell_exp.values[0]
                   + "'%s'," % income.admin_exp.values[0]
                   + "'%s'," % income.fin_exp.values[0]
                   + "'%s'," % income.assets_impair_loss.values[0]
                   + "'%s'," % income.prem_refund.values[0]
                   + "'%s'," % income.compens_payout.values[0]
                   + "'%s'," % income.reser_insur_liab.values[0]
                   + "'%s'," % income.div_payt.values[0]
                   + "'%s'," % income.reins_exp.values[0]
                   + "'%s'," % income.oper_exp.values[0]
                   + "'%s'," % income.compens_payout_refu.values[0]
                   + "'%s'," % income.insur_reser_refu.values[0]
                   + "'%s'," % income.reins_cost_refund.values[0]
                   + "'%s'," % income.other_bus_cost.values[0]
                   + "'%s'," % income.operate_profit.values[0]
                   + "'%s'," % income.non_oper_income.values[0]
                   + "'%s'," % income.non_oper_exp.values[0]
                   + "'%s'," % income.nca_disploss.values[0]
                   + "'%s'," % income.total_profit.values[0]
                   + "'%s'," % income.income_tax.values[0]
                   + "'%s'," % income.n_income.values[0]
                   + "'%s'," % income.n_income_attr_p.values[0]
                   + "'%s'," % income.minority_gain.values[0]
                   + "'%s'," % income.oth_compr_income.values[0]
                   + "'%s'," % income.t_compr_income.values[0]
                   + "'%s'," % income.compr_inc_attr_p.values[0]
                   + "'%s'," % income.compr_inc_attr_m_s.values[0]
                   + "'%s'," % income.ebit.values[0]
                   + "'%s'," % income.ebitda.values[0]
                   + "'%s'," % income.insurance_exp.values[0]
                   + "'%s'," % income.undist_profit.values[0]
                   + "'%s'" % income.distable_profit.values[0]
                   + ")")
            sql = sql.replace("'None'", "NULL").replace("'nan'", "NULL")
            print(sql)
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print(e)
    conn.close()
    cursor.close()


# 资产负债表
def collect_balancesheet(period):
    now = time.strftime('%Y%m%d', time.localtime(time.time()))

    # 连接数据库
    conn = mydb.conn()
    cursor = conn.cursor()

    cursor.execute(
        "create table if not exists balancesheet ("
        "ts_code varchar(200),"
        "ann_date varchar(200),"
        "f_ann_date varchar(200),"
        "end_date varchar(200),"
        "report_type varchar(200),"
        "comp_type varchar(200),"
        "total_share float,"
        "cap_rese float,"
        "undistr_porfit float,"
        "surplus_rese float,"
        "special_rese float,"
        "money_cap float,"
        "trad_asset float,"
        "notes_receiv float,"
        "accounts_receiv float,"
        "oth_receiv float,"
        "prepayment float,"
        "div_receiv float,"
        "int_receiv float,"
        "inventories float,"
        "amor_exp float,"
        "nca_within_1y float,"
        "sett_rsrv float,"
        "loanto_oth_bank_fi float,"
        "premium_receiv float,"
        "reinsur_receiv float,"
        "reinsur_res_receiv float,"
        "pur_resale_fa float,"
        "oth_cur_assets float,"
        "total_cur_assets float,"
        "fa_avail_for_sale float,"
        "htm_invest float,"
        "lt_eqt_invest float,"
        "invest_real_estate float,"
        "time_deposits float,"
        "oth_assets float,"
        "lt_rec float,"
        "fix_assets float,"
        "cip float,"
        "const_materials float,"
        "fixed_assets_disp float,"
        "produc_bio_assets float,"
        "oil_and_gas_assets float,"
        "intan_assets float,"
        "r_and_d float,"
        "goodwill float,"
        "lt_amor_exp float,"
        "defer_tax_assets float,"
        "decr_in_disbur float,"
        "oth_nca float,"
        "total_nca float,"
        "cash_reser_cb float,"
        "depos_in_oth_bfi float,"
        "prec_metals float,"
        "deriv_assets float,"
        "rr_reins_une_prem float,"
        "rr_reins_outstd_cla float,"
        "rr_reins_lins_liab float,"
        "rr_reins_lthins_liab float,"
        "refund_depos float,"
        "ph_pledge_loans float,"
        "refund_cap_depos float,"
        "indep_acct_assets float,"
        "client_depos float,"
        "client_prov float,"
        "transac_seat_fee float,"
        "invest_as_receiv float,"
        "total_assets float,"
        "lt_borr float,"
        "st_borr float,"
        "cb_borr float,"
        "depos_ib_deposits float,"
        "loan_oth_bank float,"
        "trading_fl float,"
        "notes_payable float,"
        "acct_payable float,"
        "adv_receipts float,"
        "sold_for_repur_fa float,"
        "comm_payable float,"
        "payroll_payable float,"
        "taxes_payable float,"
        "int_payable float,"
        "div_payable float,"
        "oth_payable float,"
        "acc_exp float,"
        "deferred_inc float,"
        "st_bonds_payable float,"
        "payable_to_reinsurer float,"
        "rsrv_insur_cont float,"
        "acting_trading_sec float,"
        "acting_uw_sec float,"
        "non_cur_liab_due_1y float,"
        "oth_cur_liab float,"
        "total_cur_liab float,"
        "bond_payable float,"
        "lt_payable float,"
        "specific_payables float,"
        "estimated_liab float,"
        "defer_tax_liab float,"
        "defer_inc_non_cur_liab float,"
        "oth_ncl float,"
        "total_ncl float,"
        "depos_oth_bfi float,"
        "deriv_liab float,"
        "depos float,"
        "agency_bus_liab float,"
        "oth_liab float,"
        "prem_receiv_adva float,"
        "depos_received float,"
        "ph_invest float,"
        "reser_une_prem float,"
        "reser_outstd_claims float,"
        "reser_lins_liab float,"
        "reser_lthins_liab float,"
        "indept_acc_liab float,"
        "pledge_borr float,"
        "indem_payable float,"
        "policy_div_payable float,"
        "total_liab float,"
        "treasury_share float,"
        "ordin_risk_reser float,"
        "forex_differ float,"
        "invest_loss_unconf float,"
        "minority_int float,"
        "total_hldr_eqy_exc_min_int float,"
        "total_hldr_eqy_inc_min_int float,"
        "total_liab_hldr_eqy float,"
        "lt_payroll_payable float,"
        "oth_comp_income float,"
        "oth_eqt_tools float,"
        "oth_eqt_tools_p_shr float,"
        "lending_funds float,"
        "acc_receivable float,"
        "st_fin_payable float,"
        "payables float,"
        "hfs_assets float,"
        "hfs_sales float,"
        "unique(ts_code,end_date))")

    # 通过for循环以及获取A股只数来遍历每一只股票
    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
    today = time.strftime('%Y%m%d')
    for index, row in stocks.iterrows():
        try:
            ts_code = row["ts_code"]

            exist_stocks = pd.read_sql("select * from balancesheet where ts_code='%s' and end_date='%s'" %
                                       (ts_code, period), conn, index_col="ts_code")
            if len(exist_stocks) > 0:
                print("%s %s balancesheet skipped." % (ts_code, period))
                continue


            balancesheet = pro.balancesheet(ts_code=ts_code)
            balancesheet = balancesheet.loc[balancesheet["end_date"] == period]

            if balancesheet is None or len(balancesheet) == 0:
                continue

            sql = ("insert into balancesheet("
                   "ts_code,"
                   "ann_date,"
                   "f_ann_date,"
                   "end_date,"
                   "report_type,"
                   "comp_type,"
                   "total_share,"
                   "cap_rese,"
                   "undistr_porfit,"
                   "surplus_rese,"
                   "special_rese,"
                   "money_cap,"
                   "trad_asset,"
                   "notes_receiv,"
                   "accounts_receiv,"
                   "oth_receiv,"
                   "prepayment,"
                   "div_receiv,"
                   "int_receiv,"
                   "inventories,"
                   "amor_exp,"
                   "nca_within_1y,"
                   "sett_rsrv,"
                   "loanto_oth_bank_fi,"
                   "premium_receiv,"
                   "reinsur_receiv,"
                   "reinsur_res_receiv,"
                   "pur_resale_fa,"
                   "oth_cur_assets,"
                   "total_cur_assets,"
                   "fa_avail_for_sale,"
                   "htm_invest,"
                   "lt_eqt_invest,"
                   "invest_real_estate,"
                   "time_deposits,"
                   "oth_assets,"
                   "lt_rec,"
                   "fix_assets,"
                   "cip,"
                   "const_materials,"
                   "fixed_assets_disp,"
                   "produc_bio_assets,"
                   "oil_and_gas_assets,"
                   "intan_assets,"
                   "r_and_d,"
                   "goodwill,"
                   "lt_amor_exp,"
                   "defer_tax_assets,"
                   "decr_in_disbur,"
                   "oth_nca,"
                   "total_nca,"
                   "cash_reser_cb,"
                   "depos_in_oth_bfi,"
                   "prec_metals,"
                   "deriv_assets,"
                   "rr_reins_une_prem,"
                   "rr_reins_outstd_cla,"
                   "rr_reins_lins_liab,"
                   "rr_reins_lthins_liab,"
                   "refund_depos,"
                   "ph_pledge_loans,"
                   "refund_cap_depos,"
                   "indep_acct_assets,"
                   "client_depos,"
                   "client_prov,"
                   "transac_seat_fee,"
                   "invest_as_receiv,"
                   "total_assets,"
                   "lt_borr,"
                   "st_borr,"
                   "cb_borr,"
                   "depos_ib_deposits,"
                   "loan_oth_bank,"
                   "trading_fl,"
                   "notes_payable,"
                   "acct_payable,"
                   "adv_receipts,"
                   "sold_for_repur_fa,"
                   "comm_payable,"
                   "payroll_payable,"
                   "taxes_payable,"
                   "int_payable,"
                   "div_payable,"
                   "oth_payable,"
                   "acc_exp,"
                   "deferred_inc,"
                   "st_bonds_payable,"
                   "payable_to_reinsurer,"
                   "rsrv_insur_cont,"
                   "acting_trading_sec,"
                   "acting_uw_sec,"
                   "non_cur_liab_due_1y,"
                   "oth_cur_liab,"
                   "total_cur_liab,"
                   "bond_payable,"
                   "lt_payable,"
                   "specific_payables,"
                   "estimated_liab,"
                   "defer_tax_liab,"
                   "defer_inc_non_cur_liab,"
                   "oth_ncl,"
                   "total_ncl,"
                   "depos_oth_bfi,"
                   "deriv_liab,"
                   "depos,"
                   "agency_bus_liab,"
                   "oth_liab,"
                   "prem_receiv_adva,"
                   "depos_received,"
                   "ph_invest,"
                   "reser_une_prem,"
                   "reser_outstd_claims,"
                   "reser_lins_liab,"
                   "reser_lthins_liab,"
                   "indept_acc_liab,"
                   "pledge_borr,"
                   "indem_payable,"
                   "policy_div_payable,"
                   "total_liab,"
                   "treasury_share,"
                   "ordin_risk_reser,"
                   "forex_differ,"
                   "invest_loss_unconf,"
                   "minority_int,"
                   "total_hldr_eqy_exc_min_int,"
                   "total_hldr_eqy_inc_min_int,"
                   "total_liab_hldr_eqy,"
                   "lt_payroll_payable,"
                   "oth_comp_income,"
                   "oth_eqt_tools,"
                   "oth_eqt_tools_p_shr,"
                   "lending_funds,"
                   "acc_receivable,"
                   "st_fin_payable,"
                   "payables,"
                   "hfs_assets,"
                   "hfs_sales"
                   ") values("
                   + "'%s'," % balancesheet.ts_code.values[0]
                   + "'%s'," % balancesheet.ann_date.values[0]
                   + "'%s'," % balancesheet.f_ann_date.values[0]
                   + "'%s'," % balancesheet.end_date.values[0]
                   + "'%s'," % balancesheet.report_type.values[0]
                   + "'%s'," % balancesheet.comp_type.values[0]
                   + "'%s'," % balancesheet.total_share.values[0]
                   + "'%s'," % balancesheet.cap_rese.values[0]
                   + "'%s'," % balancesheet.undistr_porfit.values[0]
                   + "'%s'," % balancesheet.surplus_rese.values[0]
                   + "'%s'," % balancesheet.special_rese.values[0]
                   + "'%s'," % balancesheet.money_cap.values[0]
                   + "'%s'," % balancesheet.trad_asset.values[0]
                   + "'%s'," % balancesheet.notes_receiv.values[0]
                   + "'%s'," % balancesheet.accounts_receiv.values[0]
                   + "'%s'," % balancesheet.oth_receiv.values[0]
                   + "'%s'," % balancesheet.prepayment.values[0]
                   + "'%s'," % balancesheet.div_receiv.values[0]
                   + "'%s'," % balancesheet.int_receiv.values[0]
                   + "'%s'," % balancesheet.inventories.values[0]
                   + "'%s'," % balancesheet.amor_exp.values[0]
                   + "'%s'," % balancesheet.nca_within_1y.values[0]
                   + "'%s'," % balancesheet.sett_rsrv.values[0]
                   + "'%s'," % balancesheet.loanto_oth_bank_fi.values[0]
                   + "'%s'," % balancesheet.premium_receiv.values[0]
                   + "'%s'," % balancesheet.reinsur_receiv.values[0]
                   + "'%s'," % balancesheet.reinsur_res_receiv.values[0]
                   + "'%s'," % balancesheet.pur_resale_fa.values[0]
                   + "'%s'," % balancesheet.oth_cur_assets.values[0]
                   + "'%s'," % balancesheet.total_cur_assets.values[0]
                   + "'%s'," % balancesheet.fa_avail_for_sale.values[0]
                   + "'%s'," % balancesheet.htm_invest.values[0]
                   + "'%s'," % balancesheet.lt_eqt_invest.values[0]
                   + "'%s'," % balancesheet.invest_real_estate.values[0]
                   + "'%s'," % balancesheet.time_deposits.values[0]
                   + "'%s'," % balancesheet.oth_assets.values[0]
                   + "'%s'," % balancesheet.lt_rec.values[0]
                   + "'%s'," % balancesheet.fix_assets.values[0]
                   + "'%s'," % balancesheet.cip.values[0]
                   + "'%s'," % balancesheet.const_materials.values[0]
                   + "'%s'," % balancesheet.fixed_assets_disp.values[0]
                   + "'%s'," % balancesheet.produc_bio_assets.values[0]
                   + "'%s'," % balancesheet.oil_and_gas_assets.values[0]
                   + "'%s'," % balancesheet.intan_assets.values[0]
                   + "'%s'," % balancesheet.r_and_d.values[0]
                   + "'%s'," % balancesheet.goodwill.values[0]
                   + "'%s'," % balancesheet.lt_amor_exp.values[0]
                   + "'%s'," % balancesheet.defer_tax_assets.values[0]
                   + "'%s'," % balancesheet.decr_in_disbur.values[0]
                   + "'%s'," % balancesheet.oth_nca.values[0]
                   + "'%s'," % balancesheet.total_nca.values[0]
                   + "'%s'," % balancesheet.cash_reser_cb.values[0]
                   + "'%s'," % balancesheet.depos_in_oth_bfi.values[0]
                   + "'%s'," % balancesheet.prec_metals.values[0]
                   + "'%s'," % balancesheet.deriv_assets.values[0]
                   + "'%s'," % balancesheet.rr_reins_une_prem.values[0]
                   + "'%s'," % balancesheet.rr_reins_outstd_cla.values[0]
                   + "'%s'," % balancesheet.rr_reins_lins_liab.values[0]
                   + "'%s'," % balancesheet.rr_reins_lthins_liab.values[0]
                   + "'%s'," % balancesheet.refund_depos.values[0]
                   + "'%s'," % balancesheet.ph_pledge_loans.values[0]
                   + "'%s'," % balancesheet.refund_cap_depos.values[0]
                   + "'%s'," % balancesheet.indep_acct_assets.values[0]
                   + "'%s'," % balancesheet.client_depos.values[0]
                   + "'%s'," % balancesheet.client_prov.values[0]
                   + "'%s'," % balancesheet.transac_seat_fee.values[0]
                   + "'%s'," % balancesheet.invest_as_receiv.values[0]
                   + "'%s'," % balancesheet.total_assets.values[0]
                   + "'%s'," % balancesheet.lt_borr.values[0]
                   + "'%s'," % balancesheet.st_borr.values[0]
                   + "'%s'," % balancesheet.cb_borr.values[0]
                   + "'%s'," % balancesheet.depos_ib_deposits.values[0]
                   + "'%s'," % balancesheet.loan_oth_bank.values[0]
                   + "'%s'," % balancesheet.trading_fl.values[0]
                   + "'%s'," % balancesheet.notes_payable.values[0]
                   + "'%s'," % balancesheet.acct_payable.values[0]
                   + "'%s'," % balancesheet.adv_receipts.values[0]
                   + "'%s'," % balancesheet.sold_for_repur_fa.values[0]
                   + "'%s'," % balancesheet.comm_payable.values[0]
                   + "'%s'," % balancesheet.payroll_payable.values[0]
                   + "'%s'," % balancesheet.taxes_payable.values[0]
                   + "'%s'," % balancesheet.int_payable.values[0]
                   + "'%s'," % balancesheet.div_payable.values[0]
                   + "'%s'," % balancesheet.oth_payable.values[0]
                   + "'%s'," % balancesheet.acc_exp.values[0]
                   + "'%s'," % balancesheet.deferred_inc.values[0]
                   + "'%s'," % balancesheet.st_bonds_payable.values[0]
                   + "'%s'," % balancesheet.payable_to_reinsurer.values[0]
                   + "'%s'," % balancesheet.rsrv_insur_cont.values[0]
                   + "'%s'," % balancesheet.acting_trading_sec.values[0]
                   + "'%s'," % balancesheet.acting_uw_sec.values[0]
                   + "'%s'," % balancesheet.non_cur_liab_due_1y.values[0]
                   + "'%s'," % balancesheet.oth_cur_liab.values[0]
                   + "'%s'," % balancesheet.total_cur_liab.values[0]
                   + "'%s'," % balancesheet.bond_payable.values[0]
                   + "'%s'," % balancesheet.lt_payable.values[0]
                   + "'%s'," % balancesheet.specific_payables.values[0]
                   + "'%s'," % balancesheet.estimated_liab.values[0]
                   + "'%s'," % balancesheet.defer_tax_liab.values[0]
                   + "'%s'," % balancesheet.defer_inc_non_cur_liab.values[0]
                   + "'%s'," % balancesheet.oth_ncl.values[0]
                   + "'%s'," % balancesheet.total_ncl.values[0]
                   + "'%s'," % balancesheet.depos_oth_bfi.values[0]
                   + "'%s'," % balancesheet.deriv_liab.values[0]
                   + "'%s'," % balancesheet.depos.values[0]
                   + "'%s'," % balancesheet.agency_bus_liab.values[0]
                   + "'%s'," % balancesheet.oth_liab.values[0]
                   + "'%s'," % balancesheet.prem_receiv_adva.values[0]
                   + "'%s'," % balancesheet.depos_received.values[0]
                   + "'%s'," % balancesheet.ph_invest.values[0]
                   + "'%s'," % balancesheet.reser_une_prem.values[0]
                   + "'%s'," % balancesheet.reser_outstd_claims.values[0]
                   + "'%s'," % balancesheet.reser_lins_liab.values[0]
                   + "'%s'," % balancesheet.reser_lthins_liab.values[0]
                   + "'%s'," % balancesheet.indept_acc_liab.values[0]
                   + "'%s'," % balancesheet.pledge_borr.values[0]
                   + "'%s'," % balancesheet.indem_payable.values[0]
                   + "'%s'," % balancesheet.policy_div_payable.values[0]
                   + "'%s'," % balancesheet.total_liab.values[0]
                   + "'%s'," % balancesheet.treasury_share.values[0]
                   + "'%s'," % balancesheet.ordin_risk_reser.values[0]
                   + "'%s'," % balancesheet.forex_differ.values[0]
                   + "'%s'," % balancesheet.invest_loss_unconf.values[0]
                   + "'%s'," % balancesheet.minority_int.values[0]
                   + "'%s'," % balancesheet.total_hldr_eqy_exc_min_int.values[0]
                   + "'%s'," % balancesheet.total_hldr_eqy_inc_min_int.values[0]
                   + "'%s'," % balancesheet.total_liab_hldr_eqy.values[0]
                   + "'%s'," % balancesheet.lt_payroll_payable.values[0]
                   + "'%s'," % balancesheet.oth_comp_income.values[0]
                   + "'%s'," % balancesheet.oth_eqt_tools.values[0]
                   + "'%s'," % balancesheet.oth_eqt_tools_p_shr.values[0]
                   + "'%s'," % balancesheet.lending_funds.values[0]
                   + "'%s'," % balancesheet.acc_receivable.values[0]
                   + "'%s'," % balancesheet.st_fin_payable.values[0]
                   + "'%s'," % balancesheet.payables.values[0]
                   + "'%s'," % balancesheet.hfs_assets.values[0]
                   + "'%s'" % balancesheet.hfs_sales.values[0]
                   + ")")
            sql = sql.replace("'None'", "NULL").replace("'nan'", "NULL")
            print(sql)
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print("%s%s" % (e, traceback.format_exc()))
    conn.close()
    cursor.close()


# 财务指标数据
def collect_fina_indicator_work(period, row):
    try:
        conn = mydb.conn()
        cursor = conn.cursor()

        ts_code = row["ts_code"]

        exist_stocks = pd.read_sql("select * from fina_indicator where ts_code='%s' and end_date='%s'" %
                                   (ts_code, period), conn,
                                   index_col="ts_code")
        if len(exist_stocks) > 0:
            print("%s %s fina_indicator skipped." % (ts_code, period))
            return

        df = pro.fina_indicator(ts_code=ts_code)
        df = df.loc[df["end_date"] == period]

        if df is None or len(df) == 0:
            return

        sql = ("insert into fina_indicator("

               "ts_code,"
               "ann_date,"
               "end_date,"
               "eps,"
               "dt_eps,"
               "total_revenue_ps,"
               "revenue_ps,"
               "capital_rese_ps,"
               "surplus_rese_ps,"
               "undist_profit_ps,"
               "extra_item,"
               "profit_dedt,"
               "gross_margin,"
               "current_ratio,"
               "quick_ratio,"
               "cash_ratio,"
               "invturn_days,"
               "arturn_days,"
               "inv_turn,"
               "ar_turn,"
               "ca_turn,"
               "fa_turn,"
               "assets_turn,"
               "op_income,"
               "valuechange_income,"
               "interst_income,"
               "daa,"
               "ebit,"
               "ebitda,"
               "fcff,"
               "fcfe,"
               "current_exint,"
               "noncurrent_exint,"
               "interestdebt,"
               "netdebt,"
               "tangible_asset,"
               "working_capital,"
               "networking_capital,"
               "invest_capital,"
               "retained_earnings,"
               "diluted2_eps,"
               "bps,"
               "ocfps,"
               "retainedps,"
               "cfps,"
               "ebit_ps,"
               "fcff_ps,"
               "fcfe_ps,"
               "netprofit_margin,"
               "grossprofit_margin,"
               "cogs_of_sales,"
               "expense_of_sales,"
               "profit_to_gr,"
               "saleexp_to_gr,"
               "adminexp_of_gr,"
               "finaexp_of_gr,"
               "impai_ttm,"
               "gc_of_gr,"
               "op_of_gr,"
               "ebit_of_gr,"
               "roe,"
               "roe_waa,"
               "roe_dt,"
               "roa,"
               "npta,"
               "roic,"
               "roe_yearly,"
               "roa2_yearly,"
               "roe_avg,"
               "opincome_of_ebt,"
               "investincome_of_ebt,"
               "n_op_profit_of_ebt,"
               "tax_to_ebt,"
               "dtprofit_to_profit,"
               "salescash_to_or,"
               "ocf_to_or,"
               "ocf_to_opincome,"
               "capitalized_to_da,"
               "debt_to_assets,"
               "assets_to_eqt,"
               "dp_assets_to_eqt,"
               "ca_to_assets,"
               "nca_to_assets,"
               "tbassets_to_totalassets,"
               "int_to_talcap,"
               "eqt_to_talcapital,"
               "currentdebt_to_debt,"
               "longdeb_to_debt,"
               "ocf_to_shortdebt,"
               "debt_to_eqt,"
               "eqt_to_debt,"
               "eqt_to_interestdebt,"
               "tangibleasset_to_debt,"
               "tangasset_to_intdebt,"
               "tangibleasset_to_netdebt,"
               "ocf_to_debt,"
               "ocf_to_interestdebt,"
               "ocf_to_netdebt,"
               "ebit_to_interest,"
               "longdebt_to_workingcapital,"
               "ebitda_to_debt,"
               "turn_days,"
               "roa_yearly,"
               "roa_dp,"
               "fixed_assets,"
               "profit_prefin_exp,"
               "non_op_profit,"
               "op_to_ebt,"
               "nop_to_ebt,"
               "ocf_to_profit,"
               "cash_to_liqdebt,"
               "cash_to_liqdebt_withinterest,"
               "op_to_liqdebt,"
               "op_to_debt,"
               "roic_yearly,"
               "profit_to_op,"
               "q_opincome,"
               "q_investincome,"
               "q_dtprofit,"
               "q_eps,"
               "q_netprofit_margin,"
               "q_gsprofit_margin,"
               "q_exp_to_sales,"
               "q_profit_to_gr,"
               "q_saleexp_to_gr,"
               "q_adminexp_to_gr,"
               "q_finaexp_to_gr,"
               "q_impair_to_gr_ttm,"
               "q_gc_to_gr,"
               "q_op_to_gr,"
               "q_roe,"
               "q_dt_roe,"
               "q_npta,"
               "q_opincome_to_ebt,"
               "q_investincome_to_ebt,"
               "q_dtprofit_to_profit,"
               "q_salescash_to_or,"
               "q_ocf_to_sales,"
               "q_ocf_to_or,"
               "basic_eps_yoy,"
               "dt_eps_yoy,"
               "cfps_yoy,"
               "op_yoy,"
               "ebt_yoy,"
               "netprofit_yoy,"
               "dt_netprofit_yoy,"
               "ocf_yoy,"
               "roe_yoy,"
               "bps_yoy,"
               "assets_yoy,"
               "eqt_yoy,"
               "tr_yoy,"
               "or_yoy,"
               "q_gr_yoy,"
               "q_gr_qoq,"
               "q_sales_yoy,"
               "q_sales_qoq,"
               "q_op_yoy,"
               "q_op_qoq,"
               "q_profit_yoy,"
               "q_profit_qoq,"
               "q_netprofit_yoy,"
               "q_netprofit_qoq,"
               "equity_yoy,"
               "rd_exp"
               ") values("
               + "'%s'," % ("None" if "ts_code" not in df.columns.values else df.ts_code.values[0])
               + "'%s'," % ("None" if "ann_date" not in df.columns.values else df.ann_date.values[0])
               + "'%s'," % ("None" if "end_date" not in df.columns.values else df.end_date.values[0])
               + "'%s'," % ("None" if "eps" not in df.columns.values else df.eps.values[0])
               + "'%s'," % ("None" if "dt_eps" not in df.columns.values else df.dt_eps.values[0])
               + "'%s'," % (
                   "None" if "total_revenue_ps" not in df.columns.values else df.total_revenue_ps.values[0])
               + "'%s'," % ("None" if "revenue_ps" not in df.columns.values else df.revenue_ps.values[0])
               + "'%s'," % ("None" if "capital_rese_ps" not in df.columns.values else df.capital_rese_ps.values[0])
               + "'%s'," % ("None" if "surplus_rese_ps" not in df.columns.values else df.surplus_rese_ps.values[0])
               + "'%s'," % (
                   "None" if "undist_profit_ps" not in df.columns.values else df.undist_profit_ps.values[0])
               + "'%s'," % ("None" if "extra_item" not in df.columns.values else df.extra_item.values[0])
               + "'%s'," % ("None" if "profit_dedt" not in df.columns.values else df.profit_dedt.values[0])
               + "'%s'," % ("None" if "gross_margin" not in df.columns.values else df.gross_margin.values[0])
               + "'%s'," % ("None" if "current_ratio" not in df.columns.values else df.current_ratio.values[0])
               + "'%s'," % ("None" if "quick_ratio" not in df.columns.values else df.quick_ratio.values[0])
               + "'%s'," % ("None" if "cash_ratio" not in df.columns.values else df.cash_ratio.values[0])
               + "'%s'," % ("None" if "invturn_days" not in df.columns.values else df.invturn_days.values[0])
               + "'%s'," % ("None" if "arturn_days" not in df.columns.values else df.arturn_days.values[0])
               + "'%s'," % ("None" if "inv_turn" not in df.columns.values else df.inv_turn.values[0])
               + "'%s'," % ("None" if "ar_turn" not in df.columns.values else df.ar_turn.values[0])
               + "'%s'," % ("None" if "ca_turn" not in df.columns.values else df.ca_turn.values[0])
               + "'%s'," % ("None" if "fa_turn" not in df.columns.values else df.fa_turn.values[0])
               + "'%s'," % ("None" if "assets_turn" not in df.columns.values else df.assets_turn.values[0])
               + "'%s'," % ("None" if "op_income" not in df.columns.values else df.op_income.values[0])
               + "'%s'," % (
                   "None" if "valuechange_income" not in df.columns.values else df.valuechange_income.values[0])
               + "'%s'," % ("None" if "interst_income" not in df.columns.values else df.interst_income.values[0])
               + "'%s'," % ("None" if "daa" not in df.columns.values else df.daa.values[0])
               + "'%s'," % ("None" if "ebit" not in df.columns.values else df.ebit.values[0])
               + "'%s'," % ("None" if "ebitda" not in df.columns.values else df.ebitda.values[0])
               + "'%s'," % ("None" if "fcff" not in df.columns.values else df.fcff.values[0])
               + "'%s'," % ("None" if "fcfe" not in df.columns.values else df.fcfe.values[0])
               + "'%s'," % ("None" if "current_exint" not in df.columns.values else df.current_exint.values[0])
               + "'%s'," % (
                   "None" if "noncurrent_exint" not in df.columns.values else df.noncurrent_exint.values[0])
               + "'%s'," % ("None" if "interestdebt" not in df.columns.values else df.interestdebt.values[0])
               + "'%s'," % ("None" if "netdebt" not in df.columns.values else df.netdebt.values[0])
               + "'%s'," % ("None" if "tangible_asset" not in df.columns.values else df.tangible_asset.values[0])
               + "'%s'," % ("None" if "working_capital" not in df.columns.values else df.working_capital.values[0])
               + "'%s'," % (
                   "None" if "networking_capital" not in df.columns.values else df.networking_capital.values[0])
               + "'%s'," % ("None" if "invest_capital" not in df.columns.values else df.invest_capital.values[0])
               + "'%s'," % (
                   "None" if "retained_earnings" not in df.columns.values else df.retained_earnings.values[0])
               + "'%s'," % ("None" if "diluted2_eps" not in df.columns.values else df.diluted2_eps.values[0])
               + "'%s'," % ("None" if "bps" not in df.columns.values else df.bps.values[0])
               + "'%s'," % ("None" if "ocfps" not in df.columns.values else df.ocfps.values[0])
               + "'%s'," % ("None" if "retainedps" not in df.columns.values else df.retainedps.values[0])
               + "'%s'," % ("None" if "cfps" not in df.columns.values else df.cfps.values[0])
               + "'%s'," % ("None" if "ebit_ps" not in df.columns.values else df.ebit_ps.values[0])
               + "'%s'," % ("None" if "fcff_ps" not in df.columns.values else df.fcff_ps.values[0])
               + "'%s'," % ("None" if "fcfe_ps" not in df.columns.values else df.fcfe_ps.values[0])
               + "'%s'," % (
                   "None" if "netprofit_margin" not in df.columns.values else df.netprofit_margin.values[0])
               + "'%s'," % (
                   "None" if "grossprofit_margin" not in df.columns.values else df.grossprofit_margin.values[0])
               + "'%s'," % ("None" if "cogs_of_sales" not in df.columns.values else df.cogs_of_sales.values[0])
               + "'%s'," % (
                   "None" if "expense_of_sales" not in df.columns.values else df.expense_of_sales.values[0])
               + "'%s'," % ("None" if "profit_to_gr" not in df.columns.values else df.profit_to_gr.values[0])
               + "'%s'," % ("None" if "saleexp_to_gr" not in df.columns.values else df.saleexp_to_gr.values[0])
               + "'%s'," % ("None" if "adminexp_of_gr" not in df.columns.values else df.adminexp_of_gr.values[0])
               + "'%s'," % ("None" if "finaexp_of_gr" not in df.columns.values else df.finaexp_of_gr.values[0])
               + "'%s'," % ("None" if "impai_ttm" not in df.columns.values else df.impai_ttm.values[0])
               + "'%s'," % ("None" if "gc_of_gr" not in df.columns.values else df.gc_of_gr.values[0])
               + "'%s'," % ("None" if "op_of_gr" not in df.columns.values else df.op_of_gr.values[0])
               + "'%s'," % ("None" if "ebit_of_gr" not in df.columns.values else df.ebit_of_gr.values[0])
               + "'%s'," % ("None" if "roe" not in df.columns.values else df.roe.values[0])
               + "'%s'," % ("None" if "roe_waa" not in df.columns.values else df.roe_waa.values[0])
               + "'%s'," % ("None" if "roe_dt" not in df.columns.values else df.roe_dt.values[0])
               + "'%s'," % ("None" if "roa" not in df.columns.values else df.roa.values[0])
               + "'%s'," % ("None" if "npta" not in df.columns.values else df.npta.values[0])
               + "'%s'," % ("None" if "roic" not in df.columns.values else df.roic.values[0])
               + "'%s'," % ("None" if "roe_yearly" not in df.columns.values else df.roe_yearly.values[0])
               + "'%s'," % ("None" if "roa2_yearly" not in df.columns.values else df.roa2_yearly.values[0])
               + "'%s'," % ("None" if "roe_avg" not in df.columns.values else df.roe_avg.values[0])
               + "'%s'," % ("None" if "opincome_of_ebt" not in df.columns.values else df.opincome_of_ebt.values[0])
               + "'%s'," % (
                   "None" if "investincome_of_ebt" not in df.columns.values else df.investincome_of_ebt.values[0])
               + "'%s'," % (
                   "None" if "n_op_profit_of_ebt" not in df.columns.values else df.n_op_profit_of_ebt.values[0])
               + "'%s'," % ("None" if "tax_to_ebt" not in df.columns.values else df.tax_to_ebt.values[0])
               + "'%s'," % (
                   "None" if "dtprofit_to_profit" not in df.columns.values else df.dtprofit_to_profit.values[0])
               + "'%s'," % ("None" if "salescash_to_or" not in df.columns.values else df.salescash_to_or.values[0])
               + "'%s'," % ("None" if "ocf_to_or" not in df.columns.values else df.ocf_to_or.values[0])
               + "'%s'," % ("None" if "ocf_to_opincome" not in df.columns.values else df.ocf_to_opincome.values[0])
               + "'%s'," % (
                   "None" if "capitalized_to_da" not in df.columns.values else df.capitalized_to_da.values[0])
               + "'%s'," % ("None" if "debt_to_assets" not in df.columns.values else df.debt_to_assets.values[0])
               + "'%s'," % ("None" if "assets_to_eqt" not in df.columns.values else df.assets_to_eqt.values[0])
               + "'%s'," % (
                   "None" if "dp_assets_to_eqt" not in df.columns.values else df.dp_assets_to_eqt.values[0])
               + "'%s'," % ("None" if "ca_to_assets" not in df.columns.values else df.ca_to_assets.values[0])
               + "'%s'," % ("None" if "nca_to_assets" not in df.columns.values else df.nca_to_assets.values[0])
               + "'%s'," % (
                   "None" if "tbassets_to_totalassets" not in df.columns.values else
                   df.tbassets_to_totalassets.values[0])
               + "'%s'," % ("None" if "int_to_talcap" not in df.columns.values else df.int_to_talcap.values[0])
               + "'%s'," % (
                   "None" if "eqt_to_talcapital" not in df.columns.values else df.eqt_to_talcapital.values[0])
               + "'%s'," % (
                   "None" if "currentdebt_to_debt" not in df.columns.values else df.currentdebt_to_debt.values[0])
               + "'%s'," % ("None" if "longdeb_to_debt" not in df.columns.values else df.longdeb_to_debt.values[0])
               + "'%s'," % (
                   "None" if "ocf_to_shortdebt" not in df.columns.values else df.ocf_to_shortdebt.values[0])
               + "'%s'," % ("None" if "debt_to_eqt" not in df.columns.values else df.debt_to_eqt.values[0])
               + "'%s'," % ("None" if "eqt_to_debt" not in df.columns.values else df.eqt_to_debt.values[0])
               + "'%s'," % (
                   "None" if "eqt_to_interestdebt" not in df.columns.values else df.eqt_to_interestdebt.values[0])
               + "'%s'," % (
                   "None" if "tangibleasset_to_debt" not in df.columns.values else df.tangibleasset_to_debt.values[
                       0])
               + "'%s'," % (
                   "None" if "tangasset_to_intdebt" not in df.columns.values else df.tangasset_to_intdebt.values[0])
               + "'%s'," % (
                   "None" if "tangibleasset_to_netdebt" not in df.columns.values else
                   df.tangibleasset_to_netdebt.values[0])
               + "'%s'," % ("None" if "ocf_to_debt" not in df.columns.values else df.ocf_to_debt.values[0])
               + "'%s'," % (
                   "None" if "ocf_to_interestdebt" not in df.columns.values else df.ocf_to_interestdebt.values[0])
               + "'%s'," % ("None" if "ocf_to_netdebt" not in df.columns.values else df.ocf_to_netdebt.values[0])
               + "'%s'," % (
                   "None" if "ebit_to_interest" not in df.columns.values else df.ebit_to_interest.values[0])
               + "'%s'," % ("None" if "longdebt_to_workingcapital" not in df.columns.values else
                            df.longdebt_to_workingcapital.values[0])
               + "'%s'," % ("None" if "ebitda_to_debt" not in df.columns.values else df.ebitda_to_debt.values[0])
               + "'%s'," % ("None" if "turn_days" not in df.columns.values else df.turn_days.values[0])
               + "'%s'," % ("None" if "roa_yearly" not in df.columns.values else df.roa_yearly.values[0])
               + "'%s'," % ("None" if "roa_dp" not in df.columns.values else df.roa_dp.values[0])
               + "'%s'," % ("None" if "fixed_assets" not in df.columns.values else df.fixed_assets.values[0])
               + "'%s'," % (
                   "None" if "profit_prefin_exp" not in df.columns.values else df.profit_prefin_exp.values[0])
               + "'%s'," % ("None" if "non_op_profit" not in df.columns.values else df.non_op_profit.values[0])
               + "'%s'," % ("None" if "op_to_ebt" not in df.columns.values else df.op_to_ebt.values[0])
               + "'%s'," % ("None" if "nop_to_ebt" not in df.columns.values else df.nop_to_ebt.values[0])
               + "'%s'," % ("None" if "ocf_to_profit" not in df.columns.values else df.ocf_to_profit.values[0])
               + "'%s'," % ("None" if "cash_to_liqdebt" not in df.columns.values else df.cash_to_liqdebt.values[0])
               + "'%s'," % ("None" if "cash_to_liqdebt_withinterest" not in df.columns.values else
                            df.cash_to_liqdebt_withinterest.values[0])
               + "'%s'," % ("None" if "op_to_liqdebt" not in df.columns.values else df.op_to_liqdebt.values[0])
               + "'%s'," % ("None" if "op_to_debt" not in df.columns.values else df.op_to_debt.values[0])
               + "'%s'," % ("None" if "roic_yearly" not in df.columns.values else df.roic_yearly.values[0])
               + "'%s'," % ("None" if "profit_to_op" not in df.columns.values else df.profit_to_op.values[0])
               + "'%s'," % ("None" if "q_opincome" not in df.columns.values else df.q_opincome.values[0])
               + "'%s'," % ("None" if "q_investincome" not in df.columns.values else df.q_investincome.values[0])
               + "'%s'," % ("None" if "q_dtprofit" not in df.columns.values else df.q_dtprofit.values[0])
               + "'%s'," % ("None" if "q_eps" not in df.columns.values else df.q_eps.values[0])
               + "'%s'," % (
                   "None" if "q_netprofit_margin" not in df.columns.values else df.q_netprofit_margin.values[0])
               + "'%s'," % (
                   "None" if "q_gsprofit_margin" not in df.columns.values else df.q_gsprofit_margin.values[0])
               + "'%s'," % ("None" if "q_exp_to_sales" not in df.columns.values else df.q_exp_to_sales.values[0])
               + "'%s'," % ("None" if "q_profit_to_gr" not in df.columns.values else df.q_profit_to_gr.values[0])
               + "'%s'," % ("None" if "q_saleexp_to_gr" not in df.columns.values else df.q_saleexp_to_gr.values[0])
               + "'%s'," % (
                   "None" if "q_adminexp_to_gr" not in df.columns.values else df.q_adminexp_to_gr.values[0])
               + "'%s'," % ("None" if "q_finaexp_to_gr" not in df.columns.values else df.q_finaexp_to_gr.values[0])
               + "'%s'," % (
                   "None" if "q_impair_to_gr_ttm" not in df.columns.values else df.q_impair_to_gr_ttm.values[0])
               + "'%s'," % ("None" if "q_gc_to_gr" not in df.columns.values else df.q_gc_to_gr.values[0])
               + "'%s'," % ("None" if "q_op_to_gr" not in df.columns.values else df.q_op_to_gr.values[0])
               + "'%s'," % ("None" if "q_roe" not in df.columns.values else df.q_roe.values[0])
               + "'%s'," % ("None" if "q_dt_roe" not in df.columns.values else df.q_dt_roe.values[0])
               + "'%s'," % ("None" if "q_npta" not in df.columns.values else df.q_npta.values[0])
               + "'%s'," % (
                   "None" if "q_opincome_to_ebt" not in df.columns.values else df.q_opincome_to_ebt.values[0])
               + "'%s'," % (
                   "None" if "q_investincome_to_ebt" not in df.columns.values else df.q_investincome_to_ebt.values[
                       0])
               + "'%s'," % (
                   "None" if "q_dtprofit_to_profit" not in df.columns.values else df.q_dtprofit_to_profit.values[0])
               + "'%s'," % (
                   "None" if "q_salescash_to_or" not in df.columns.values else df.q_salescash_to_or.values[0])
               + "'%s'," % ("None" if "q_ocf_to_sales" not in df.columns.values else df.q_ocf_to_sales.values[0])
               + "'%s'," % ("None" if "q_ocf_to_or" not in df.columns.values else df.q_ocf_to_or.values[0])
               + "'%s'," % ("None" if "basic_eps_yoy" not in df.columns.values else df.basic_eps_yoy.values[0])
               + "'%s'," % ("None" if "dt_eps_yoy" not in df.columns.values else df.dt_eps_yoy.values[0])
               + "'%s'," % ("None" if "cfps_yoy" not in df.columns.values else df.cfps_yoy.values[0])
               + "'%s'," % ("None" if "op_yoy" not in df.columns.values else df.op_yoy.values[0])
               + "'%s'," % ("None" if "ebt_yoy" not in df.columns.values else df.ebt_yoy.values[0])
               + "'%s'," % ("None" if "netprofit_yoy" not in df.columns.values else df.netprofit_yoy.values[0])
               + "'%s'," % (
                   "None" if "dt_netprofit_yoy" not in df.columns.values else df.dt_netprofit_yoy.values[0])
               + "'%s'," % ("None" if "ocf_yoy" not in df.columns.values else df.ocf_yoy.values[0])
               + "'%s'," % ("None" if "roe_yoy" not in df.columns.values else df.roe_yoy.values[0])
               + "'%s'," % ("None" if "bps_yoy" not in df.columns.values else df.bps_yoy.values[0])
               + "'%s'," % ("None" if "assets_yoy" not in df.columns.values else df.assets_yoy.values[0])
               + "'%s'," % ("None" if "eqt_yoy" not in df.columns.values else df.eqt_yoy.values[0])
               + "'%s'," % ("None" if "tr_yoy" not in df.columns.values else df.tr_yoy.values[0])
               + "'%s'," % ("None" if "or_yoy" not in df.columns.values else df.or_yoy.values[0])
               + "'%s'," % ("None" if "q_gr_yoy" not in df.columns.values else df.q_gr_yoy.values[0])
               + "'%s'," % ("None" if "q_gr_qoq" not in df.columns.values else df.q_gr_qoq.values[0])
               + "'%s'," % ("None" if "q_sales_yoy" not in df.columns.values else df.q_sales_yoy.values[0])
               + "'%s'," % ("None" if "q_sales_qoq" not in df.columns.values else df.q_sales_qoq.values[0])
               + "'%s'," % ("None" if "q_op_yoy" not in df.columns.values else df.q_op_yoy.values[0])
               + "'%s'," % ("None" if "q_op_qoq" not in df.columns.values else df.q_op_qoq.values[0])
               + "'%s'," % ("None" if "q_profit_yoy" not in df.columns.values else df.q_profit_yoy.values[0])
               + "'%s'," % ("None" if "q_profit_qoq" not in df.columns.values else df.q_profit_qoq.values[0])
               + "'%s'," % ("None" if "q_netprofit_yoy" not in df.columns.values else df.q_netprofit_yoy.values[0])
               + "'%s'," % ("None" if "q_netprofit_qoq" not in df.columns.values else df.q_netprofit_qoq.values[0])
               + "'%s'," % ("None" if "equity_yoy" not in df.columns.values else df.equity_yoy.values[0])
               + "'%s'" % ("None" if "rd_exp" not in df.columns.values else df.rd_exp.values[0])
               + ")")
        sql = sql.replace("'None'", "NULL").replace("'nan'", "NULL")
        print(sql)
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        print(e)
    conn.close()
    cursor.close()

def collect_fina_indicator(period):
    now = time.strftime('%Y%m%d', time.localtime(time.time()))

    # 连接数据库
    conn = mydb.conn()
    cursor = conn.cursor()

    cursor.execute(
        "create table if not exists fina_indicator ("
        "ts_code varchar(200),"
        "ann_date varchar(200),"
        "end_date varchar(200),"
        "eps float,"
        "dt_eps float,"
        "total_revenue_ps float,"
        "revenue_ps float,"
        "capital_rese_ps float,"
        "surplus_rese_ps float,"
        "undist_profit_ps float,"
        "extra_item float,"
        "profit_dedt float,"
        "gross_margin float,"
        "current_ratio float,"
        "quick_ratio float,"
        "cash_ratio float,"
        "invturn_days float,"
        "arturn_days float,"
        "inv_turn float,"
        "ar_turn float,"
        "ca_turn float,"
        "fa_turn float,"
        "assets_turn float,"
        "op_income float,"
        "valuechange_income float,"
        "interst_income float,"
        "daa float,"
        "ebit float,"
        "ebitda float,"
        "fcff float,"
        "fcfe float,"
        "current_exint float,"
        "noncurrent_exint float,"
        "interestdebt float,"
        "netdebt float,"
        "tangible_asset float,"
        "working_capital float,"
        "networking_capital float,"
        "invest_capital float,"
        "retained_earnings float,"
        "diluted2_eps float,"
        "bps float,"
        "ocfps float,"
        "retainedps float,"
        "cfps float,"
        "ebit_ps float,"
        "fcff_ps float,"
        "fcfe_ps float,"
        "netprofit_margin float,"
        "grossprofit_margin float,"
        "cogs_of_sales float,"
        "expense_of_sales float,"
        "profit_to_gr float,"
        "saleexp_to_gr float,"
        "adminexp_of_gr float,"
        "finaexp_of_gr float,"
        "impai_ttm float,"
        "gc_of_gr float,"
        "op_of_gr float,"
        "ebit_of_gr float,"
        "roe float,"
        "roe_waa float,"
        "roe_dt float,"
        "roa float,"
        "npta float,"
        "roic float,"
        "roe_yearly float,"
        "roa2_yearly float,"
        "roe_avg float,"
        "opincome_of_ebt float,"
        "investincome_of_ebt float,"
        "n_op_profit_of_ebt float,"
        "tax_to_ebt float,"
        "dtprofit_to_profit float,"
        "salescash_to_or float,"
        "ocf_to_or float,"
        "ocf_to_opincome float,"
        "capitalized_to_da float,"
        "debt_to_assets float,"
        "assets_to_eqt float,"
        "dp_assets_to_eqt float,"
        "ca_to_assets float,"
        "nca_to_assets float,"
        "tbassets_to_totalassets float,"
        "int_to_talcap float,"
        "eqt_to_talcapital float,"
        "currentdebt_to_debt float,"
        "longdeb_to_debt float,"
        "ocf_to_shortdebt float,"
        "debt_to_eqt float,"
        "eqt_to_debt float,"
        "eqt_to_interestdebt float,"
        "tangibleasset_to_debt float,"
        "tangasset_to_intdebt float,"
        "tangibleasset_to_netdebt float,"
        "ocf_to_debt float,"
        "ocf_to_interestdebt float,"
        "ocf_to_netdebt float,"
        "ebit_to_interest float,"
        "longdebt_to_workingcapital float,"
        "ebitda_to_debt float,"
        "turn_days float,"
        "roa_yearly float,"
        "roa_dp float,"
        "fixed_assets float,"
        "profit_prefin_exp float,"
        "non_op_profit float,"
        "op_to_ebt float,"
        "nop_to_ebt float,"
        "ocf_to_profit float,"
        "cash_to_liqdebt float,"
        "cash_to_liqdebt_withinterest float,"
        "op_to_liqdebt float,"
        "op_to_debt float,"
        "roic_yearly float,"
        "profit_to_op float,"
        "q_opincome float,"
        "q_investincome float,"
        "q_dtprofit float,"
        "q_eps float,"
        "q_netprofit_margin float,"
        "q_gsprofit_margin float,"
        "q_exp_to_sales float,"
        "q_profit_to_gr float,"
        "q_saleexp_to_gr float,"
        "q_adminexp_to_gr float,"
        "q_finaexp_to_gr float,"
        "q_impair_to_gr_ttm float,"
        "q_gc_to_gr float,"
        "q_op_to_gr float,"
        "q_roe float,"
        "q_dt_roe float,"
        "q_npta float,"
        "q_opincome_to_ebt float,"
        "q_investincome_to_ebt float,"
        "q_dtprofit_to_profit float,"
        "q_salescash_to_or float,"
        "q_ocf_to_sales float,"
        "q_ocf_to_or float,"
        "basic_eps_yoy float,"
        "dt_eps_yoy float,"
        "cfps_yoy float,"
        "op_yoy float,"
        "ebt_yoy float,"
        "netprofit_yoy float,"
        "dt_netprofit_yoy float,"
        "ocf_yoy float,"
        "roe_yoy float,"
        "bps_yoy float,"
        "assets_yoy float,"
        "eqt_yoy float,"
        "tr_yoy float,"
        "or_yoy float,"
        "q_gr_yoy float,"
        "q_gr_qoq float,"
        "q_sales_yoy float,"
        "q_sales_qoq float,"
        "q_op_yoy float,"
        "q_op_qoq float,"
        "q_profit_yoy float,"
        "q_profit_qoq float,"
        "q_netprofit_yoy float,"
        "q_netprofit_qoq float,"
        "equity_yoy float,"
        "rd_exp float,"
        "unique(ts_code,end_date))")

    # 通过for循环以及获取A股只数来遍历每一只股票
    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
    today = time.strftime('%Y%m%d')
    with ThreadPoolExecutor(10) as executor:
        for index, row in stocks.iterrows():
            executor.submit(collect_fina_indicator_work, period, row)

    conn.close()
    cursor.close()


if __name__ == '__main__':
    daily.collect_daily()
    # daily_basic.collect_daily_basic()
    # daily.collect_daily_qfq()

    period_info = mydate.get_period_info()
    period_date = period_info[0]
    # price_period.collect_price(period_date, False)
    # collect_disclosure(period_date)
    collect_fina_indicator(period_date)
    collect_income(period_date)
    collect_balancesheet(period_date)

    for year in range(2009, 2019):
        for md in ["0331", "0630", "0930", "1231"]:
            period_date = str(year) + md
            # price_period.collect_price(period_date, False)
            # collect_disclosure(period_date)
            collect_fina_indicator(period_date)
            collect_income(period_date)
            collect_balancesheet(period_date)
    '''
    period_date = "20180930"
    price_period.collect_price(period_date, False)
    collect_disclosure(period_date)
    collect_income(period_date)
    collect_balancesheet(period_date)
    collect_fina_indicator(period_date)

    price_period.collect_price("20190331", False)
    for year in range(2017, 2019):
        for md in ["0331", "0630", "0930", "1231"]:
            period_date = str(year) + md
            price_period.collect_price(period_date, False)
            
    '''
