
WITH diffs AS (
	SELECT
		product_num,
		trade_date,
		net_value,
		LAG(net_value, 250) OVER (
			PARTITION BY
				product_num
			ORDER BY
				trade_date
		) previous_net_value_y
	FROM
		iproduct_valuation
)
SELECT
	product_num,
	trade_date,
	net_value,
	previous_net_value_y,
	(net_value - previous_net_value_y) chg_y,
	((net_value - previous_net_value_y) / NULLIF (previous_net_value_y, 0)) chg_rate_y,
	to_char(((net_value - previous_net_value_y) / NULLIF (previous_net_value_y, 0) * 10000 ), 'fm00D00%%') pb_fmt_y
FROM
	diffs
