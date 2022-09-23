package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.TimeSeries
import regime.market.Common._

object AShareYield extends TimeSeries {
  lazy val query = RegimeSqlHelper.fromResource("sql/market/timeseries/AShareYield.sql")
  lazy val queryFromDate = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(query, Token.timeColumnMarket, date)
  lazy val queryDateRange = (fromDate: String, toDate: String) =>
    RegimeSqlHelper.generateQueryDateRange(query, Token.timeColumnMarket, (fromDate, toDate))

  lazy val readFrom       = connMarketTable("ASHAREYIELD")
  lazy val saveTo         = connBizTable("ashare_yield")
  lazy val readFromCol    = connMarketTableColumn("ASHAREYIELD", Token.timeColumnMarket)
  lazy val saveToCol      = connBizTableColumn("ashare_yield", Token.timeColumnBiz)
  lazy val primaryKeyName = "PK_ashare_yield"
  lazy val primaryColumn  = Seq(Token.objectId)
  lazy val index1         = ("IDX_ashare_yield_1", Seq(Token.timeColumnBiz))
  lazy val index2         = ("IDX_ashare_yield_2", Seq(Token.timeTradeDate, Token.symbol))

  lazy val conversionFn = RegimeFn.formatStringToDate(Token.timeTradeDate, dateFormat)

  def process(args: String*)(implicit spark: SparkSession): Unit =
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None, conversionFn)
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
          None,
          conversionFn
        )
      case Command.OverrideFromLastUpdate :: _ =>
        syncUpsertFromLastUpdate(
          readFromCol,
          saveToCol,
          primaryColumn,
          queryFromDate,
          None,
          None,
          conversionFn
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          readFrom,
          saveTo,
          queryFromDate(timeFrom),
          primaryColumn,
          None,
          conversionFn
        )
      case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
        syncUpsert(
          readFrom,
          saveTo,
          queryFromDate(timeFrom),
          primaryColumn,
          None,
          conversionFn
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
}
