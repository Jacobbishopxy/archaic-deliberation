
SELECT
  beb.holidayMonth AS accounting_period,
  beb.tradeDate AS trade_date,
  beb.productNum AS product_num,
  beb.subjectId AS subject_id,
  beb.subjectName AS subject_name,
  beb.PreviousQtyF AS previous_qty,
  beb.PostQtyF AS post_qty,
  beb.PreviousAmt AS previous_amt,
  beb.PostAmt AS post_amt,
  beb.previousStandardAmt AS previous_std_amt,
  beb.currentStandardAmt AS current_std_amt,
  beb.DebitAmt AS debit_amt,
  beb.CreditAmt AS credit_amt,
  beb.AssetFlag AS asset_flag,
  beb.YTotalOpenQtyF AS year_total_open_qty,
  beb.YTotalCloseQtyF AS year_total_close_qty,
  bp.productName AS product_name,
  bp.NAV AS nav,
  bp.CurrentAsset AS post_asset
FROM
  bside_ev_balancehis beb
LEFT JOIN
  bside_product bp
ON
  beb.productNum  = bp.productNum
