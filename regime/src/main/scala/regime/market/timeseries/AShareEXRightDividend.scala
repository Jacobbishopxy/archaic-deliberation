package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.TimeSeries
import regime.market.Common._

object AShareEXRightDividend extends TimeSeries {
  lazy val query = RegimeSqlHelper.fromResource("sql/market/timeseries/AShareEXRightDividend.sql")
  lazy val queryFromDate = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(query, Token.timeColumnMarket, date)
  lazy val queryDateRange = (fromDate: String, toDate: String) =>
    RegimeSqlHelper.generateQueryDateRange(query, Token.timeColumnMarket, (fromDate, toDate))

  lazy val readFrom = connMarketTable("ASHAREEXRIGHTDIVIDENDRECORD")
  lazy val saveTo   = connBizTable("ashare_ex_right_dividend_record")
  lazy val readFromCol =
    connMarketTableColumn("ASHAREEXRIGHTDIVIDENDRECORD", Token.timeColumnMarket)
  lazy val saveToCol = connBizTableColumn("ashare_ex_right_dividend_record", Token.timeColumnBiz)
  lazy val primaryKeyName = "PK_ashare_ex_right_dividend_record"
  lazy val primaryColumn  = Seq(Token.objectId)
  lazy val index          = ("IDX_ashare_ex_right_dividend_record", Seq(Token.timeColumnBiz))

  def process(args: String*)(implicit spark: SparkSession): Unit =
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None)
        createPrimaryKeyAndIndex(
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq(index)
        )
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq(index)
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
