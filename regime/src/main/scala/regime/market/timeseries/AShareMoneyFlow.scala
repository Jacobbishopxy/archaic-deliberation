package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.TimeSeries
import regime.market.Common._

object AShareMoneyFlow extends TimeSeries {
  lazy val query = RegimeSqlHelper.fromResource("sql/market/timeseries/AShareMoneyFlow.sql")
  lazy val queryFromDate = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(query, Token.timeColumnMarket, date)
  lazy val queryDateRange = (fromDate: String, toDate: String) =>
    RegimeSqlHelper.generateQueryDateRange(query, Token.timeColumnMarket, (fromDate, toDate))

  lazy val readFrom       = connMarketTable("ASHAREMONEYFLOW")
  lazy val saveTo         = connBizTable("ashare_money_flow")
  lazy val readFromCol    = connMarketTableColumn("ASHAREMONEYFLOW", Token.timeColumnMarket)
  lazy val saveToCol      = connBizTableColumn("ashare_money_flow", Token.timeColumnBiz)
  lazy val primaryKeyName = "PK_ashare_money_flow"
  lazy val primaryColumn  = Seq(Token.objectId)
  lazy val index1         = ("IDX_ashare_money_flow_1", Seq(Token.timeColumnBiz))
  lazy val index2         = ("IDX_ashare_money_flow_2", Seq(Token.timeTradeDate, Token.symbol))

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
          queryDateRange(timeFrom, timeTo),
          primaryColumn,
          None,
          conversionFn
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
}
