package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.TimeSeries
import regime.market.Common._

object AShareL2Indicator extends RegimeSpark with TimeSeries {
  lazy val query = RegimeSqlHelper.fromResource("sql/market/timeseries/AShareL2Indicator.sql")
  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """
  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  lazy val readFrom       = connMarketTable("ASHAREL2INDICATORS")
  lazy val saveTo         = connBizTable("ashare_level2_indicator")
  lazy val readFromCol    = connMarketTableColumn("ASHAREL2INDICATORS", "OPDATE")
  lazy val saveToCol      = connBizTableColumn("ashare_level2_indicator", "update_date")
  lazy val primaryKeyName = "PK_ashare_level2_indicator"
  lazy val primaryColumn  = Seq("object_id")
  lazy val index1         = ("IDX_ashare_level2_indicator1", Seq("update_date"))
  lazy val index2         = ("IDX_ashare_level2_indicator2", Seq("trade_date", "symbol"))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None)
        createPrimaryKeyAndIndex(
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq(index1, index2)
        )
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq(index1, index2)
        )
      case Command.SyncFromLastUpdate :: _ =>
        syncInsertFromLastUpdate(
          readFromCol,
          saveToCol,
          queryFromDate,
          None,
          None
        )
      case Command.OverrideFromLastUpdate :: _ =>
        syncUpsertFromLastUpdate(
          readFromCol,
          saveToCol,
          primaryColumn,
          queryFromDate,
          None,
          None
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          readFrom,
          saveTo,
          queryFromDate(timeFrom),
          primaryColumn,
          None
        )
      case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
        syncUpsert(
          readFrom,
          saveTo,
          queryDateRange(timeFrom, timeTo),
          primaryColumn,
          None
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
  }

}
