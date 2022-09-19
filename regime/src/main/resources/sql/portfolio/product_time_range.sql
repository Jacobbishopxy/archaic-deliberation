
SELECT
	product_id,
	COUNT(*) AS count,
	MIN(iv.trade_date) AS start_date,
	MAX(iv.trade_date) AS end_date
FROM
	iproduct_valuation iv
GROUP BY
	product_id
ORDER BY
	end_date
