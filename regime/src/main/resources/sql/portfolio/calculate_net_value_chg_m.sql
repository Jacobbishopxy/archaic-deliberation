
WITH diffs AS (
	SELECT
		product_num,
		trade_date,
		net_value,
		LAG(net_value, 20) OVER (
			PARTITION BY
				product_num
			ORDER BY
				trade_date
		) previous_net_value_m
	FROM
		iproduct_valuation
)
SELECT
	product_num,
	trade_date,
	net_value,
	previous_net_value_m,
	(net_value - previous_net_value_m) chg_m,
	((net_value - previous_net_value_m) / NULLIF (previous_net_value_m, 0)) chg_rate_m,
	to_char(((net_value - previous_net_value_m) / NULLIF (previous_net_value_m, 0) * 10000 ), 'fm00D00%%') pb_fmt_m
FROM
	diffs
