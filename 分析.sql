-- ROE与预收款的反比例(ROE正)
-- ROE为负，证明历史收益率低
-- 预收款多了，证明业绩可能好转
SELECT
	*
FROM
	(
SELECT
	a.ts_code,
	a.start_trade_date,
	a.close1,
	a.close_min,
	a.close_min_date,
	a.close_max,
	a.close_max_date,
	a.close2,
	a.end_trade_date,
	a.pe,
	f.roe,
	f.grossprofit_margin 毛利率,
	b.accounts_receiv,
	b.adv_receipts,
	( IF ( close_max > close1, close_max, close2 ) - close1 ) / close1 * 100 AS up_rate,
	b.adv_receipts / i.total_revenue AS 预收款率,
	b.accounts_receiv 应收账款,
	b.accounts_receiv / i.total_revenue AS 应收账款率,
	i.n_income 净利润,
	i.n_income / i.total_revenue 净利润率,
	i.total_revenue 营业总收入,
	b.total_assets 总资产,
	f.assets_turn 总资产周转率,
	b.total_hldr_eqy_inc_min_int 股东权益,
	b.total_hldr_eqy_inc_min_int/b.total_assets 股东权益比率,
	(abs(f.roe) * (b.adv_receipts / i.total_revenue - b.accounts_receiv / i.total_revenue) * (i.n_income / i.total_revenue) * (b.total_hldr_eqy_inc_min_int/b.total_assets ) ) as order1
FROM
	price_period a
	JOIN balancesheet b ON a.ts_code = b.ts_code
	AND a.end_date = '20190331'
	AND b.end_date = '20190331'
	JOIN income i ON a.ts_code = i.ts_code
	AND i.end_date = '20190331'
	JOIN fina_indicator f ON a.ts_code = f.ts_code
	AND f.end_date = '20190331'
	) a
WHERE
	1 = 1
	AND 营业总收入 > 应收账款
	AND roe > 0
	AND roe / 毛利率 > 0.3
	AND pe < 65 AND 净利润率 > 0.3
	-- AND order1 > 1
ORDER BY
	order1 DESC




-- 关键指标
SELECT
	*
FROM
	(
SELECT
	a.ts_code,
	d.actual_date,
	-- a.start_trade_date,
	-- a.close1,
	-- a.close_min,
	-- a.close_min_date,
	-- a.close_max,
	-- a.close_max_date,
	-- a.close2,
	-- a.end_trade_date,
	a.pe,
	f.roe,
	f.grossprofit_margin 毛利率,
	-- b.accounts_receiv,
	-- b.adv_receipts,
	( IF ( close_max > close1, close_max, close2 ) - close1 ) / close1 * 100 AS up_rate,
	b.adv_receipts / i.total_revenue AS 预收款率,
	-- b.accounts_receiv 应收账款,
	b.accounts_receiv / i.total_revenue AS 应收账款率,
	-- i.n_income 净利润,
	i.n_income / i.total_revenue 净利润率,
	-- i.total_revenue 营业总收入,
	-- b.total_assets 总资产,
	f.assets_turn 总资产周转率,
	-- b.total_hldr_eqy_inc_min_int 股东权益,
	b.total_hldr_eqy_inc_min_int/b.total_assets 股东权益比率
FROM
	price_period a
	JOIN balancesheet b ON a.ts_code = b.ts_code
	AND a.end_date = '20190331'
	AND b.end_date = a.end_date
	JOIN income i ON a.ts_code = i.ts_code
	AND i.end_date = a.end_date
	JOIN fina_indicator f ON a.ts_code = f.ts_code
	AND f.end_date = a.end_date
	join disclosure d on a.ts_code = d.ts_code
	and d.end_date=a.end_date
	) a
WHERE
	1 = 1
  and a.ts_code in ('603309.SH','002038.SZ','000590.SZ','002287.SZ','601958.SH','002932.SZ','002320.SZ','600317.SH','002393.SZ')
ORDER BY
	up_rate DESC