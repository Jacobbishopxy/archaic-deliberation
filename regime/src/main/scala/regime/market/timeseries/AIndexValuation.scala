package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.TimeSeries
import regime.market.Common._

object AIndexValuation extends TimeSeries {
  lazy val query = RegimeSqlHelper.fromResource("sql/market/timeseries/AIndexValuation.sql")
  lazy val queryFromDate = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(query, Token.timeColumnMarket, date)
  lazy val queryDateRange = (fromDate: String, toDate: String) =>
    RegimeSqlHelper.generateQueryDateRange(query, Token.timeColumnMarket, (fromDate, toDate))

  lazy val readFrom       = connMarketTable("AINDEXVALUATION")
  lazy val saveTo         = connBizTable("aindex_valuation")
  lazy val readFromCol    = connMarketTableColumn("AINDEXVALUATION", Token.timeColumnMarket)
  lazy val saveToCol      = connBizTableColumn("aindex_valuation", Token.timeColumnBiz)
  lazy val primaryKeyName = "PK_aindex_valuation"
  lazy val primaryColumn  = Seq(Token.objectId)
  lazy val index1         = ("IDX_aindex_valuation_1", Seq(Token.timeColumnBiz))
  lazy val index2         = ("IDX_aindex_valuation_2", Seq(Token.timeTradeDate, Token.symbol))

  lazy val conversionFn = RegimeFn.formatStringToDate(Token.timeTradeDate, dateFormat)

  def process(args: String*)(implicit spark: SparkSession): Unit = {
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
}
