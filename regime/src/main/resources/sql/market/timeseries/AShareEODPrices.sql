
SELECT
  OBJECT_ID AS object_id,
  S_INFO_WINDCODE AS symbol,
  TRADE_DT AS trade_date,
  CRNCY_CODE AS currency,
  S_DQ_PRECLOSE AS pre_close,
  S_DQ_OPEN AS open_price,
  S_DQ_HIGH AS high_price,
  S_DQ_LOW AS low_price,
  S_DQ_CLOSE AS close_price,
  S_DQ_CHANGE AS change,
  S_DQ_PCTCHANGE AS percent_change,
  S_DQ_VOLUME AS volume,
  S_DQ_AMOUNT AS amount,
  S_DQ_ADJPRECLOSE AS adj_pre_close,
  S_DQ_ADJOPEN AS adj_open,
  S_DQ_ADJHIGH AS adj_high,
  S_DQ_ADJLOW AS adj_low,
  S_DQ_ADJCLOSE AS adj_close,
  S_DQ_ADJFACTOR AS adj_factor,
  S_DQ_AVGPRICE AS average_price,
  S_DQ_TRADESTATUS AS trade_status,
  S_DQ_TRADESTATUSCODE AS trade_status_code,
  S_DQ_LIMIT AS limit_price,
  S_DQ_STOPPING AS stopping_price,
  OPDATE AS update_date
FROM
  ASHAREEODPRICES
