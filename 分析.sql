-- ROE与预收款的反比例(ROE负)
-- ROE为负，证明历史收益率低
-- 预收款多了，证明业绩可能好转
SELECT
	*
FROM
	(
SELECT
	*,
	( if(close_max>close1,close_max,close2) - close1 ) / close1 * 100 AS up_rate,
	roe / grossprofit_margin  roe_gross,
	adv_receipts / total_revenue AS rec_rev,
	n_income / total_revenue AS inc_rev ,
	(-(roe) * (adv_receipts / total_revenue) )  as order1
FROM
	stock_analyze_1804 a
	) a
WHERE
	1 = 1
	and roe < 1 and roe > -100
	and pe <65
ORDER BY
	order1  desc

-- ROE与预收款的反比例(ROE正)
-- ROE为负，证明历史收益率低
-- 预收款多了，证明业绩可能好转
SELECT
	*
FROM
	(
SELECT
	*,
	( if(close_max>close1,close_max,close2) - close1 ) / close1 * 100 AS up_rate,
	roe / grossprofit_margin  roe_gross,
	adv_receipts / total_revenue AS rec_rev,
	accounts_receiv / total_revenue AS acc_rev,
	n_income / total_revenue AS inc_rev ,
	(abs(roe) * (adv_receipts / total_revenue)) as order1
FROM
	stock_analyze_1801 a
	) a
WHERE
	1 = 1
	and total_revenue>accounts_receiv
	and roe > 0
	and roe_gross>0.5
	and pe <65
	and inc_rev > 0.3
ORDER BY
	order1  desc