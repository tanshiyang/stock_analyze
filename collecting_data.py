import mytusharepro
import mydb
import pandas as pd
import mysql.connector
import re, time
import mydate
import tradeday

pro = mytusharepro.MyTusharePro()


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


#资产负债表
def collect_balancesheet(period):
    now = time.strftime('%Y%m%d', time.localtime(time.time()))
    disclosure_date = pro.disclosure_date(end_date='%s' % period)
    disclosure_date = disclosure_date[disclosure_date.actual_date < now]
    ts_codes = disclosure_date.ts_code

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
        "unique(ts_code,ann_date))")

    # 通过for循环以及获取A股只数来遍历每一只股票
    for x in ts_codes.index:
        try:
            balancesheet = pro.balancesheet(ts_code=ts_codes[x], end_date=period)
            if balancesheet is None or len(balancesheet) == 0:
                continue

            exist_stocks = pd.read_sql("select * from balancesheet where ts_code='%s' and ann_date='%s'" %
                                       (balancesheet.ts_code[0], balancesheet.ann_date[0]), conn, index_col="ts_code")
            if len(exist_stocks) > 0:
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
                   + "'%s'," % balancesheet.ts_code[0]
                   + "'%s'," % balancesheet.ann_date[0]
                   + "'%s'," % balancesheet.f_ann_date[0]
                   + "'%s'," % balancesheet.end_date[0]
                   + "'%s'," % balancesheet.report_type[0]
                   + "'%s'," % balancesheet.comp_type[0]
                   + "'%s'," % balancesheet.total_share[0]
                   + "'%s'," % balancesheet.cap_rese[0]
                   + "'%s'," % balancesheet.undistr_porfit[0]
                   + "'%s'," % balancesheet.surplus_rese[0]
                   + "'%s'," % balancesheet.special_rese[0]
                   + "'%s'," % balancesheet.money_cap[0]
                   + "'%s'," % balancesheet.trad_asset[0]
                   + "'%s'," % balancesheet.notes_receiv[0]
                   + "'%s'," % balancesheet.accounts_receiv[0]
                   + "'%s'," % balancesheet.oth_receiv[0]
                   + "'%s'," % balancesheet.prepayment[0]
                   + "'%s'," % balancesheet.div_receiv[0]
                   + "'%s'," % balancesheet.int_receiv[0]
                   + "'%s'," % balancesheet.inventories[0]
                   + "'%s'," % balancesheet.amor_exp[0]
                   + "'%s'," % balancesheet.nca_within_1y[0]
                   + "'%s'," % balancesheet.sett_rsrv[0]
                   + "'%s'," % balancesheet.loanto_oth_bank_fi[0]
                   + "'%s'," % balancesheet.premium_receiv[0]
                   + "'%s'," % balancesheet.reinsur_receiv[0]
                   + "'%s'," % balancesheet.reinsur_res_receiv[0]
                   + "'%s'," % balancesheet.pur_resale_fa[0]
                   + "'%s'," % balancesheet.oth_cur_assets[0]
                   + "'%s'," % balancesheet.total_cur_assets[0]
                   + "'%s'," % balancesheet.fa_avail_for_sale[0]
                   + "'%s'," % balancesheet.htm_invest[0]
                   + "'%s'," % balancesheet.lt_eqt_invest[0]
                   + "'%s'," % balancesheet.invest_real_estate[0]
                   + "'%s'," % balancesheet.time_deposits[0]
                   + "'%s'," % balancesheet.oth_assets[0]
                   + "'%s'," % balancesheet.lt_rec[0]
                   + "'%s'," % balancesheet.fix_assets[0]
                   + "'%s'," % balancesheet.cip[0]
                   + "'%s'," % balancesheet.const_materials[0]
                   + "'%s'," % balancesheet.fixed_assets_disp[0]
                   + "'%s'," % balancesheet.produc_bio_assets[0]
                   + "'%s'," % balancesheet.oil_and_gas_assets[0]
                   + "'%s'," % balancesheet.intan_assets[0]
                   + "'%s'," % balancesheet.r_and_d[0]
                   + "'%s'," % balancesheet.goodwill[0]
                   + "'%s'," % balancesheet.lt_amor_exp[0]
                   + "'%s'," % balancesheet.defer_tax_assets[0]
                   + "'%s'," % balancesheet.decr_in_disbur[0]
                   + "'%s'," % balancesheet.oth_nca[0]
                   + "'%s'," % balancesheet.total_nca[0]
                   + "'%s'," % balancesheet.cash_reser_cb[0]
                   + "'%s'," % balancesheet.depos_in_oth_bfi[0]
                   + "'%s'," % balancesheet.prec_metals[0]
                   + "'%s'," % balancesheet.deriv_assets[0]
                   + "'%s'," % balancesheet.rr_reins_une_prem[0]
                   + "'%s'," % balancesheet.rr_reins_outstd_cla[0]
                   + "'%s'," % balancesheet.rr_reins_lins_liab[0]
                   + "'%s'," % balancesheet.rr_reins_lthins_liab[0]
                   + "'%s'," % balancesheet.refund_depos[0]
                   + "'%s'," % balancesheet.ph_pledge_loans[0]
                   + "'%s'," % balancesheet.refund_cap_depos[0]
                   + "'%s'," % balancesheet.indep_acct_assets[0]
                   + "'%s'," % balancesheet.client_depos[0]
                   + "'%s'," % balancesheet.client_prov[0]
                   + "'%s'," % balancesheet.transac_seat_fee[0]
                   + "'%s'," % balancesheet.invest_as_receiv[0]
                   + "'%s'," % balancesheet.total_assets[0]
                   + "'%s'," % balancesheet.lt_borr[0]
                   + "'%s'," % balancesheet.st_borr[0]
                   + "'%s'," % balancesheet.cb_borr[0]
                   + "'%s'," % balancesheet.depos_ib_deposits[0]
                   + "'%s'," % balancesheet.loan_oth_bank[0]
                   + "'%s'," % balancesheet.trading_fl[0]
                   + "'%s'," % balancesheet.notes_payable[0]
                   + "'%s'," % balancesheet.acct_payable[0]
                   + "'%s'," % balancesheet.adv_receipts[0]
                   + "'%s'," % balancesheet.sold_for_repur_fa[0]
                   + "'%s'," % balancesheet.comm_payable[0]
                   + "'%s'," % balancesheet.payroll_payable[0]
                   + "'%s'," % balancesheet.taxes_payable[0]
                   + "'%s'," % balancesheet.int_payable[0]
                   + "'%s'," % balancesheet.div_payable[0]
                   + "'%s'," % balancesheet.oth_payable[0]
                   + "'%s'," % balancesheet.acc_exp[0]
                   + "'%s'," % balancesheet.deferred_inc[0]
                   + "'%s'," % balancesheet.st_bonds_payable[0]
                   + "'%s'," % balancesheet.payable_to_reinsurer[0]
                   + "'%s'," % balancesheet.rsrv_insur_cont[0]
                   + "'%s'," % balancesheet.acting_trading_sec[0]
                   + "'%s'," % balancesheet.acting_uw_sec[0]
                   + "'%s'," % balancesheet.non_cur_liab_due_1y[0]
                   + "'%s'," % balancesheet.oth_cur_liab[0]
                   + "'%s'," % balancesheet.total_cur_liab[0]
                   + "'%s'," % balancesheet.bond_payable[0]
                   + "'%s'," % balancesheet.lt_payable[0]
                   + "'%s'," % balancesheet.specific_payables[0]
                   + "'%s'," % balancesheet.estimated_liab[0]
                   + "'%s'," % balancesheet.defer_tax_liab[0]
                   + "'%s'," % balancesheet.defer_inc_non_cur_liab[0]
                   + "'%s'," % balancesheet.oth_ncl[0]
                   + "'%s'," % balancesheet.total_ncl[0]
                   + "'%s'," % balancesheet.depos_oth_bfi[0]
                   + "'%s'," % balancesheet.deriv_liab[0]
                   + "'%s'," % balancesheet.depos[0]
                   + "'%s'," % balancesheet.agency_bus_liab[0]
                   + "'%s'," % balancesheet.oth_liab[0]
                   + "'%s'," % balancesheet.prem_receiv_adva[0]
                   + "'%s'," % balancesheet.depos_received[0]
                   + "'%s'," % balancesheet.ph_invest[0]
                   + "'%s'," % balancesheet.reser_une_prem[0]
                   + "'%s'," % balancesheet.reser_outstd_claims[0]
                   + "'%s'," % balancesheet.reser_lins_liab[0]
                   + "'%s'," % balancesheet.reser_lthins_liab[0]
                   + "'%s'," % balancesheet.indept_acc_liab[0]
                   + "'%s'," % balancesheet.pledge_borr[0]
                   + "'%s'," % balancesheet.indem_payable[0]
                   + "'%s'," % balancesheet.policy_div_payable[0]
                   + "'%s'," % balancesheet.total_liab[0]
                   + "'%s'," % balancesheet.treasury_share[0]
                   + "'%s'," % balancesheet.ordin_risk_reser[0]
                   + "'%s'," % balancesheet.forex_differ[0]
                   + "'%s'," % balancesheet.invest_loss_unconf[0]
                   + "'%s'," % balancesheet.minority_int[0]
                   + "'%s'," % balancesheet.total_hldr_eqy_exc_min_int[0]
                   + "'%s'," % balancesheet.total_hldr_eqy_inc_min_int[0]
                   + "'%s'," % balancesheet.total_liab_hldr_eqy[0]
                   + "'%s'," % balancesheet.lt_payroll_payable[0]
                   + "'%s'," % balancesheet.oth_comp_income[0]
                   + "'%s'," % balancesheet.oth_eqt_tools[0]
                   + "'%s'," % balancesheet.oth_eqt_tools_p_shr[0]
                   + "'%s'," % balancesheet.lending_funds[0]
                   + "'%s'," % balancesheet.acc_receivable[0]
                   + "'%s'," % balancesheet.st_fin_payable[0]
                   + "'%s'," % balancesheet.payables[0]
                   + "'%s'," % balancesheet.hfs_assets[0]
                   + "'%s'" % balancesheet.hfs_sales[0]
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
    # collect_income(period_date)
    collect_balancesheet(period_date)
