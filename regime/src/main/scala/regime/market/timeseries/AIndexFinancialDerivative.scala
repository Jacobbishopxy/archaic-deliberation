package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.TimeSeries
import regime.market.Common._

object AIndexFinancialDerivative extends TimeSeries {
  lazy val query =
    RegimeSqlHelper.fromResource("sql/market/timeseries/AIndexFinancialDerivative.sql")
  lazy val queryFromDate = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(query, "OPDATE", date)
  lazy val queryDateRange = (fromDate: String, toDate: String) =>
    RegimeSqlHelper.generateQueryDateRange(query, "OPDATE", (fromDate, toDate))

  lazy val readFrom       = connMarketTable("AINDEXFINANCIALDERIVATIVE")
  lazy val saveTo         = connBizTable("aindex_financial_derivative")
  lazy val readFromCol    = connMarketTableColumn("AINDEXFINANCIALDERIVATIVE", "OPDATE")
  lazy val saveToCol      = connBizTableColumn("aindex_financial_derivative", "update_date")
  lazy val primaryKeyName = "PK_aindex_financial_derivative"
  lazy val primaryColumn  = Seq("object_id")
  lazy val index1         = ("IDX_aindex_financial_derivative_1", Seq("update_date"))
  lazy val index2         = ("IDX_aindex_financial_derivative_2", Seq("report_period", "symbol"))

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
