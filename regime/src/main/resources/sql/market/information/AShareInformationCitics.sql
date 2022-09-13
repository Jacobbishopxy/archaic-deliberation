
SELECT
  ad.OBJECT_ID AS object_id,
  ad.S_INFO_WINDCODE AS symbol,
  ad.S_INFO_NAME AS name,
  ad.S_INFO_COMPNAME AS company_name,
  ad.S_INFO_COMPNAMEENG AS company_name_eng,
  ad.S_INFO_EXCHMARKET AS exchange,
  ad.S_INFO_LISTBOARD AS listboard,
  ad.S_INFO_LISTDATE AS listdate,
  ad.S_INFO_DELISTDATE AS delistdate,
  ad.S_INFO_PINYIN AS pinyin,
  ad.S_INFO_LISTBOARDNAME AS listboard_name,
  ad.IS_SHSC AS is_shsc,
  aim.S_INFO_WINDCODE AS wind_ind_code,
  aim.S_CON_INDATE AS entry_date,
  aim.S_CON_OUTDATE AS remove_date,
  aim.CUR_SIGN AS cur_sign,
  ic.S_INFO_INDUSTRYCODE AS industry_code,
  ic.S_INFO_INDUSTRYNAME AS industry_name
FROM
  ASHAREDESCRIPTION ad
LEFT JOIN
  AINDEXMEMBERSCITICS aim
ON
  ad.S_INFO_WINDCODE = aim.S_CON_WINDCODE
LEFT JOIN
  INDEXCONTRASTSECTOR ic
ON
  aim.S_INFO_WINDCODE = ic.S_INFO_INDEXCODE
