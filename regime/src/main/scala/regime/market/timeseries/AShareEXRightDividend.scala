package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.TimeSeries
import regime.market.Common._

object AShareEXRightDividend extends RegimeSpark with TimeSeries {
  lazy val query = """
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

  lazy val readFrom       = "ASHAREEXRIGHTDIVIDENDRECORD"
  lazy val saveTo         = "ashare_ex_right_dividend_record"
  lazy val readUpdateCol  = "OPDATE"
  lazy val saveUpdateCol  = "update_date"
  lazy val primaryKeyName = "PK_ashare_ex_right_dividend_record"
  lazy val primaryColumn  = Seq("object_id")
  lazy val index          = ("IDX_ashare_ex_right_dividend_record", Seq("update_date"))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(connMarket, query, connBizTable(saveTo))
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
          (primaryKeyName, primaryColumn),
          Seq(index)
        )
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
          (primaryKeyName, primaryColumn),
          Seq(index)
        )
      case Command.SyncFromLastUpdate :: _ =>
        syncInsertFromLastUpdate(
          connMarketTableColumn(readFrom, readUpdateCol),
          connBizTableColumn(saveTo, saveUpdateCol),
          queryFromDate
        )
      case Command.OverrideFromLastUpdate :: _ =>
        syncUpsertFromLastUpdate(
          connMarketTableColumn(readFrom, readUpdateCol),
          connBizTableColumn(saveTo, saveUpdateCol),
          primaryColumn,
          queryFromDate
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          connMarket,
          queryFromDate(timeFrom),
          connBizTable(saveTo),
          primaryColumn
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
  }

}
