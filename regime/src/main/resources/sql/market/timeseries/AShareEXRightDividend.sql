
SELECT
  OBJECT_ID as object_id,
  S_INFO_WINDCODE as symbol,
  EX_DATE as ex_date,
  EX_TYPE as ex_type,
  EX_DESCRIPTION as ex_description,
  CASH_DIVIDEND_RATIO as cash_dividend_ratio,
  BONUS_SHARE_RATIO as bonus_share_ratio,
  RIGHTSISSUE_RATIO as right_issue_ratio,
  RIGHTSISSUE_PRICE as right_issue_price,
  CONVERSED_RATIO as conversed_ratio,
  SEO_PRICE as seo_price,
  SEO_RATIO as seo_ratio,
  CONSOLIDATE_SPLIT_RATIO as consolidate_split_ratio,
  OPDATE as update_date
FROM
  ASHAREEXRIGHTDIVIDENDRECORD
