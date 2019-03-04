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
	(abs(roe) * (adv_receipts / total_revenue) )  as order1
FROM
	stock_analyze_1803 a
	) a
WHERE
	1 = 1
	and roe < 1 and roe > -100
	and pe <65
ORDER BY
	order1  desc