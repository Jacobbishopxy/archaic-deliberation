
SELECT
  OBJECT_ID as object_id,
  S_INFO_WINDCODE AS symbol,
  S_DQ_SUSPENDDATE AS suspend_date,
  S_DQ_SUSPENDTYPE AS suspend_type,
  S_DQ_RESUMPDATE AS resume_date,
  S_DQ_CHANGEREASON AS reason,
  S_DQ_TIME AS suspend_time,
  OPDATE AS update_date
FROM
  ASHARETRADINGSUSPENSION
