
SELECT
  ad.OBJECT_ID AS object_id_ad,
  aic.OBJECT_ID AS object_id_aic,
  ac.OBJECT_ID AS object_id_ac,
  ad.S_INFO_WINDCODE AS symbol,
  ad.S_INFO_NAME AS name,
  ad.S_INFO_COMPNAME AS company_name,
  ad.S_INFO_COMPNAMEENG  AS company_name_eng,
  ad.S_INFO_EXCHMARKET AS exchange,
  ad.S_INFO_LISTBOARD AS list_board,
  ad.S_INFO_LISTDATE AS list_date,
  ad.S_INFO_DELISTDATE AS delist_date,
  ad.S_INFO_PINYIN AS pinyin,
  ad.S_INFO_LISTBOARDNAME AS list_board_name,
  ad.IS_SHSC AS is_shsc,
  aic.WIND_IND_CODE AS wind_ind_code,
  aic.ENTRY_DT AS entry_date,
  aic.REMOVE_DT AS remove_date,
  aic.CUR_SIGN AS cur_sign,
  ac.INDUSTRIESCODE AS industry_code,
  ac.INDUSTRIESNAME AS industry_name,
  ac.LEVELNUM AS industry_level,
  ad.OPDATE AS update_date
FROM
  ASHAREDESCRIPTION ad
LEFT JOIN
  ASHAREINDUSTRIESCLASS aic
ON
  ad.S_INFO_WINDCODE = aic.S_INFO_WINDCODE
LEFT JOIN
  ASHAREINDUSTRIESCODE ac
ON
  aic.WIND_IND_CODE = ac.INDUSTRIESCODE
