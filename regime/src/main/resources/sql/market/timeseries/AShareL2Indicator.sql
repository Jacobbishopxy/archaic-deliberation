
SELECT
  OBJECT_ID AS object_id,
  S_INFO_WINDCODE AS symbol,
  TRADE_DT AS trade_date,
  S_LI_INITIATIVEBUYRATE AS initiative_buy_rate,
  S_LI_INITIATIVEBUYMONEY AS initiative_buy_money,
  S_LI_INITIATIVEBUYAMOUNT AS initiative_buy_amount,
  S_LI_INITIATIVESELLRATE AS initiative_sell_rate,
  S_LI_INITIATIVESELLMONEY AS initiative_sell_money,
  S_LI_INITIATIVESELLAMOUNT AS initiative_sell_amount,
  S_LI_LARGEBUYRATE AS large_buy_rate,
  S_LI_LARGEBUYMONEY AS large_buy_money,
  S_LI_LARGEBUYAMOUNT AS large_buy_amount,
  S_LI_LARGESELLRATE AS large_sell_rate,
  S_LI_LARGESELLMONEY AS large_sell_money,
  S_LI_LARGESELLAMOUNT AS large_sell_amount,
  S_LI_ENTRUSTRATE AS entrust_rate,
  S_LI_ENTRUDIFFERAMOUNT AS entrust_differ_amount,
  S_LI_ENTRUDIFFERAMONEY AS entrust_differ_amoney,
  S_LI_ENTRUSTBUYMONEY AS entrust_buy_money,
  S_LI_ENTRUSTSELLMONEY AS entrust_sell_money,
  S_LI_ENTRUSTBUYAMOUNT AS entrust_buy_amount,
  S_LI_ENTRUSTSELLAMOUNT AS entrust_sell_amount,
  OPDATE AS update_date
FROM
  ASHAREL2INDICATORS
