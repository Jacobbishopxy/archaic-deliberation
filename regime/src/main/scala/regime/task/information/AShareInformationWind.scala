package regime.task.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.SparkTaskCommon
import regime.task.Common.{connMarket, connBiz}
import regime.helper.RegimeJdbcHelper

object AShareInformationWind extends SparkTaskCommon {
  val appName: String = "AShareInformationWind ETL"

  val query = """
  SELECT
    ad.OBJECT_ID as object_id,
    ad.S_INFO_WINDCODE as symbol,
    ad.S_INFO_NAME as name,
    ad.S_INFO_COMPNAME as company_name,
    ad.S_INFO_COMPNAMEENG  as company_name_eng,
    ad.S_INFO_EXCHMARKET as exchange,
    ad.S_INFO_LISTBOARD as listboard,
    ad.S_INFO_LISTDATE as listdate,
    ad.S_INFO_DELISTDATE as delistdate,
    ad.S_INFO_PINYIN as pinyin,
    ad.S_INFO_LISTBOARDNAME as listboard_name,
    ad.IS_SHSC as is_shsc,
    aic.WIND_IND_CODE as wind_ind_code,
    aic.ENTRY_DT as entry_date,
    aic.REMOVE_DT as remove_date,
    aic.CUR_SIGN as cur_sign,
    ac.INDUSTRIESCODE as industry_code,
    ac.INDUSTRIESNAME as industry_name,
    ac.LEVELNUM as industry_level
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
  WHERE
    ac.USED = 1
  """

  val count_current_available = """
  SELECT
    COUNT(*)
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
  WHERE
    ac.USED = 1 AND aic.CUR_SIGN = 1
  """

  val save_to = "ashare_information_wind"

  def process(spark: SparkSession): Unit = {
    // Read from source
    val df = RegimeJdbcHelper(connMarket).readTable(spark, query)

    // Save to the target
    RegimeJdbcHelper(connBiz).saveTable(df, save_to, SaveMode.Overwrite)
  }
}
