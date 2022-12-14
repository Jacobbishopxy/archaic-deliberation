
SELECT
  tradeDate AS trade_date,
  productNum AS product_num,
  productName AS product_name,
  parentProductNum AS parent_product_num,
  prodAcctType AS product_account_type,
  exchId AS exch_id,
  stkId AS stock_id,
  stkName AS stock_name,
  stkType AS stock_type,
  stkType2 AS stock_type2,
  f_productId AS product_id,
  StkProperty AS stock_property,
  bsFlag AS bs_flag,
  previousQtyF AS previous_qty,
  PrePositionCost AS pre_position_cost,
  previousStkValue AS previous_stk_value,
  TDTotalOpenQty AS tdy_total_open_qty,
  TDTotalOpenAmt AS tdy_total_open_amt,
  TDOpenReckoningAmt AS tdy_open_reckoning_amt,
  TDTotalOpenFee AS tdy_total_open_fee,
  TDOpenAvgPrice AS tdy_open_avg_price,
  AssetsBuyTotalQty AS assets_buy_total_qty,
  TotalOpenAmt AS total_open_amt,
  tdTotalCloseQty AS tdy_total_close_qty,
  tdTotalCloseAmt AS tdy_total_close_amt,
  TDCloseReckoningAmt AS tdy_close_reckoning_amt,
  tdTotalCloseFee AS tdy_total_close_fee,
  TDCloseAvgPrice AS tdy_close_avg_price,
  returnStkQty AS return_stock_qty,
  AssetsSellTotalQty AS assets_sell_total_qty,
  TotalCloseAmt AS total_close_amt,
  PostQtyF AS post_qty,
  PostCostAmt AS post_cost_amt,
  PostAmt AS post_amt,
  PostCostAmtContainFee AS post_cost_amt_with_fee,
  SettlePostCostAmtContainFee AS settle_post_cost_amt_with_fee,
  marginUsedAmt AS margin_used_amt,
  CostPrice AS cost_price,
  CostPriceContainFee AS cost_price_with_fee,
  stkValue AS stock_value,
  currentStkValue AS current_stock_value,
  closePNL AS close_pnl,
  sumClosePNL AS sum_close_pnl,
  sumClosePNLContainFee AS sum_close_pnl_with_fee,
  floatPnl AS float_pnl,
  floatPnlContainFee AS float_pnl_with_fee,
  todayPNL AS today_pnl,
  PNL_ContributionRate AS pnl_contribution_rate,
  BPBuy AS bp_buy,
  BPSell AS bp_sell,
  TotalMktKnockQty AS total_market_knock_qty,
  TotalMktKnockAmt AS total_market_knock_amt,
  knockAvgPrice AS knock_avg_price,
  closePrice AS close_price,
  preSettlementPrice AS pre_settlement_price,
  targetType AS target_type,
  F_hedgeFlag AS hedge_flag,
  SuspendedFlag AS suspended_flag,
  ExchRate AS exch_rate,
  buyExchRate AS buy_exch_rate,
  sellExchRate AS sell_exch_rate,
  ContractTimes AS contract_times,
  postCostRate AS post_cost_rate,
  investRate AS invest_rate,
  UnrealizedDiviDendQty AS unrealized_dividend_qty,
  QtyF1 AS qty_f1,
  QtyF2 AS qty_f2,
  Amt1 AS amt1,
  Amt1ContainFee AS amt1_with_fee,
  StkValue1 AS stock_value1,
  ChargeAmt AS charge_amt,
  SettleDividendAmt AS settle_dividend_amt,
  WindCode AS wind_code,
  BBTick AS bb_tick,
  QtyF3 AS qty_f3,
  Amt3 AS amt3,
  StkValue3 AS stock_value3
FROM
  bside_ev_rpt_tradesummary
