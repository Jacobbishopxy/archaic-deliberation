package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, TimeSeries, RegimeTask}
import regime.market.Common.{connMarket, connBiz}

object AIndexEODPricesWind extends RegimeTask with TimeSeries {
  val appName: String = "AIndexEODPricesWind"

  val query = """
  SELECT
    OBJECT_ID AS object_id,
    S_INFO_WINDCODE AS symbol,
    TRADE_DT AS trade_date,
    CRNCY_CODE AS currency,
    S_DQ_PRECLOSE AS pre_close,
    S_DQ_OPEN AS open_price,
    S_DQ_HIGH AS high_price,
    S_DQ_LOW AS low_price,
    S_DQ_CLOSE AS close_price,
    S_DQ_CHANGE AS change,
    S_DQ_PCTCHANGE AS percent_change,
    S_DQ_VOLUME AS volume,
    S_DQ_AMOUNT AS amount,
    OPDATE AS update_date
  FROM
    AINDEXWINDINDUSTRIESEOD
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  val saveTo         = "aindex_eod_prices_wind"
  val primaryKeyName = "PK_aindex_eod_prices_wind"
  val primaryColumn  = Seq("object_id")
  val index1         = ("IDX_aindex_eod_prices_wind_1", Seq("update_date"))
  val index2         = ("IDX_aindex_eod_prices_wind_2", Seq("trade_date", "symbol"))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.SyncAll :: _ =>
        syncAll(connMarket, query, connBiz, saveTo)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBiz,
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq(index1, index2)
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          connMarket,
          queryFromDate(timeFrom),
          connBiz,
          primaryColumn,
          saveTo
        )
      case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
        syncUpsert(
          connMarket,
          queryDateRange(timeFrom, timeTo),
          connBiz,
          primaryColumn,
          saveTo
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
  }
}
