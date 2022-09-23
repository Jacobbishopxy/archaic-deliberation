
WITH diffs AS (
	SELECT
		product_num,
		trade_date,
		net_value,
		LAG(net_value, 60) OVER (
			PARTITION BY
				product_num
			ORDER BY
				trade_date
		) previous_net_value_q
	FROM
		iproduct_valuation
)
SELECT
	product_num,
	trade_date,
	net_value,
	previous_net_value_q,
	(net_value - previous_net_value_q) chg_q,
	((net_value - previous_net_value_q) / NULLIF (previous_net_value_q, 0)) chg_rate_q,
	to_char(((net_value - previous_net_value_q) / NULLIF (previous_net_value_q, 0) * 10000 ), 'fm00D00%%') pb_fmt_q
FROM
	diffs
