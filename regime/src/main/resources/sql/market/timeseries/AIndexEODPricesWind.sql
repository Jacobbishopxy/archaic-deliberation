
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
  OPDATE AS update_date
FROM
  AINDEXWINDINDUSTRIESEOD
