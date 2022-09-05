package regime.task.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.SparkTaskCommon
import regime.task.Common.{connMarket, connBiz}
import regime.helper.RegimeJdbcHelper
import regime.Command

object AShareInformationCitics extends SparkTaskCommon {
  val appName: String = "AShareInformationCitics ETL"

  val query = """
  SELECT TOP 5
    ad.OBJECT_ID as object_id,
    ad.S_INFO_WINDCODE as symbol,
    ad.S_INFO_NAME as name,
    ad.S_INFO_COMPNAME as company_name,
    ad.S_INFO_COMPNAMEENG as company_name_eng,
    ad.S_INFO_EXCHMARKET as exchange,
    ad.S_INFO_LISTBOARD as listboard,
    ad.S_INFO_LISTDATE as listdate,
    ad.S_INFO_DELISTDATE as delistdate,
    ad.S_INFO_PINYIN as pinyin,
    ad.S_INFO_LISTBOARDNAME as listboard_name,
    ad.IS_SHSC as is_shsc,
    aim.S_INFO_WINDCODE as wind_ind_code,
    aim.S_CON_INDATE as entry_date,
    aim.S_CON_OUTDATE as remove_date,
    aim.CUR_SIGN as cur_sign,
    ic.S_INFO_INDUSTRYCODE as industry_code,
    ic.S_INFO_INDUSTRYNAME as industry_name
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
  """

  val count_current_available = """
  SELECT
    COUNT(*)
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
  WHERE
    aim.CUR_SIGN = 1
  """

  val saveTo         = "ashare_information_scitics"
  val primaryKeyName = "PK_ashare_information_scitics"
  val primaryColumn  = Seq("object_id")

  def process(spark: SparkSession, args: String*): Unit = {
    args.toList match {
      case Command.SyncAll :: _     => syncAll(spark)
      case Command.ExecuteOnce :: _ => createPrimaryKey()
      case _                        => throw new Exception("Invalid command")
    }
  }

  private def syncAll(spark: SparkSession): Unit = {
    // Read from source
    val df = RegimeJdbcHelper(connMarket).readTable(spark, query)

    // Save to the target
    RegimeJdbcHelper(connBiz).saveTable(df, saveTo, SaveMode.Overwrite)
  }

  private def createPrimaryKey(): Unit =
    RegimeJdbcHelper(connBiz).createPrimaryKey(saveTo, primaryKeyName, primaryColumn)

}
