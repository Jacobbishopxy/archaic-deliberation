
SELECT
  ad.OBJECT_ID AS object_id,
  am.F_INFO_WINDCODE AS index_symbol,
  am.S_CON_WINDCODE AS symbol,
  am.S_CON_INDATE AS in_date,
  am.S_CON_OUTDATE AS out_date,
  am.CUR_SIGN AS cur_sign,
  am.OPDATE AS update_date,
  ad.S_INFO_NAME AS index_abbr,
  ad.S_INFO_COMPNAME AS index_name,
  ad.S_INFO_EXCHMARKET AS exchange,
  ad.S_INFO_INDEX_BASEPER AS index_base_per,
  ad.S_INFO_INDEX_BASEPT AS index_base_pt,
  ad.S_INFO_LISTDATE AS list_date,
  ad.S_INFO_INDEX_WEIGHTSRULE AS index_weights_rule,
  ad.S_INFO_PUBLISHER AS publisher,
  ad.S_INFO_INDEXCODE AS index_code,
  ad.S_INFO_INDEXSTYLE AS index_style,
  ad.INDEX_INTRO AS index_intro,
  ad.WEIGHT_TYPE AS weight_type,
  ad.EXPIRE_DATE AS expire_date,
  ad.OPDATE AS description_update_date
FROM
  AINDEXMEMBERSWIND am
LEFT JOIN
  AINDEXDESCRIPTION ad
ON
  am.F_INFO_WINDCODE = ad.S_INFO_WINDCODE
