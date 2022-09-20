
WITH diffs AS (
	SELECT
		product_num,
		trade_date,
		net_value,
		LAG(net_value, 1) OVER (
			PARTITION BY
				product_num
			ORDER BY
				trade_date
		) previous_net_value
	FROM
		iproduct_valuation
)
SELECT
	product_num,
	trade_date,
	net_value,
	previous_net_value,
	(net_value - previous_net_value) chg,
	((net_value - previous_net_value) / NULLIF (previous_net_value, 0)) chg_rate,
	to_char(((net_value - previous_net_value) / NULLIF (previous_net_value, 0) * 10000 ), 'fm00D00%%') pb_chg
FROM
	diffs
