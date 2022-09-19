package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.TimeSeries
import regime.market.Common._

object AIndexEODPricesCitics extends TimeSeries {
  lazy val query = RegimeSqlHelper.fromResource("sql/market/timeseries/AIndexEODPricesCitics.sql")
  lazy val queryFromDate = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(query, timeColumnMarket, date)
  lazy val queryDateRange = (fromDate: String, toDate: String) =>
    RegimeSqlHelper.generateQueryDateRange(query, timeColumnMarket, (fromDate, toDate))

  lazy val readFrom       = connMarketTable("AINDEXINDUSTRIESEODCITICS")
  lazy val saveTo         = connBizTable("aindex_eod_prices_citics")
  lazy val readFromCol    = connMarketTableColumn("AINDEXINDUSTRIESEODCITICS", timeColumnMarket)
  lazy val saveToCol      = connBizTableColumn("aindex_eod_prices_citics", timeColumnBiz)
  lazy val primaryKeyName = "PK_aindex_eod_prices_citics"
  lazy val primaryColumn  = Seq("object_id")
  lazy val index1         = ("IDX_aindex_eod_prices_citics_1", Seq(timeColumnBiz))
  lazy val index2         = ("IDX_aindex_eod_prices_citics_2", Seq(timeTradeDate, "symbol"))

  lazy val conversionFn = RegimeFn.formatStringToDate(timeTradeDate, dateFormat)

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
}
