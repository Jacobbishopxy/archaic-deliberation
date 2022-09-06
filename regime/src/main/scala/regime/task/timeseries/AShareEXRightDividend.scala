package regime.task.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.task.{Command, TimeSeries, RegimeTask}
import regime.task.Common.{connMarket, connBiz}

object AShareEXRightDividend extends RegimeTask with TimeSeries {
  val appName: String = "AShareEXRightDividend"

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

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.SyncAll :: _ =>
        syncAll(connMarket, query, connBiz, saveTo)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBiz,
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq((indexName, indexColumn))
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          connMarket,
          queryFromDate(timeFrom),
          connBiz,
          primaryColumn,
          saveTo
        )
      case _ =>
        throw new Exception("Invalid command")
    }
  }

}
