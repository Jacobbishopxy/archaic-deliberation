package regime.task.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.SparkTaskCommon
import regime.task.Common.{connMarket, connBiz}
import regime.helper.RegimeJdbcHelper
import regime.Command

// TODO:
// 1. append
// 1. replace
object AShareEXRightDividend extends SparkTaskCommon {
  val appName: String = "AShareEXRightDividend ETL"

  val query = """
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
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  val saveTo         = "ashare_ex_right_dividend_record"
  val primaryKeyName = "PK_ashare_ex_right_dividend_record"
  val primaryColumn  = Seq("object_id")
  val indexName      = "IDX_ashare_ex_right_dividend_record"
  val indexColumn    = Seq("update_date")

  def process(spark: SparkSession, args: String*): Unit = {
    args.toList match {
      case Command.SyncAll :: _                    => syncAll(spark)
      case Command.ExecuteOnce :: _                => createPrimaryKeyAndIndex()
      case Command.TimeFromUpsert :: timeFrom :: _ => upsertFromDate(spark, timeFrom)
      case _                                       => throw new Exception("Invalid command")
    }
  }

  private def syncAll(spark: SparkSession): Unit = {
    val df = RegimeJdbcHelper(connMarket).readTable(spark, query)

    RegimeJdbcHelper(connBiz).saveTable(df, saveTo, SaveMode.Overwrite)
  }

  private def createPrimaryKeyAndIndex(): Unit = {
    val helper = RegimeJdbcHelper(connBiz)

    helper.createPrimaryKey(saveTo, primaryKeyName, primaryColumn)

    helper.createIndex(saveTo, indexName, indexColumn)
  }

  private def upsertFromDate(spark: SparkSession, fromDate: String): Unit = {
    val df = RegimeJdbcHelper(connMarket).readTable(spark, queryFromDate(fromDate))

    RegimeJdbcHelper(connBiz).upsertTable(df, saveTo, None, false, primaryColumn, "doUpdate")
  }

}
