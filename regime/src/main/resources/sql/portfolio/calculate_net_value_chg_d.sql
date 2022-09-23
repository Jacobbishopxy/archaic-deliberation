
WITH diffs AS (
	SELECT
		product_num,
		product_id,
		product_name,
		trade_date,
		net_value,
		LAG(net_value, 1) OVER (
			PARTITION BY
				product_num
			ORDER BY
				trade_date
		) previous_net_value_d
	FROM
		iproduct_valuation
)
SELECT
	product_num,
	product_id,
	product_name,
	trade_date,
	net_value,
	previous_net_value_d,
	(net_value - previous_net_value_d) chg_d,
	((net_value - previous_net_value_d) / NULLIF (previous_net_value_d, 0)) chg_rate_d,
	to_char(((net_value - previous_net_value_d) / NULLIF (previous_net_value_d, 0) * 10000 ), 'fm00D00%%') pb_fmt_d
FROM
	diffs
