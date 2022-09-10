package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, TimeSeries, RegimeTask}
import regime.market.Common.{connMarket, connBiz, connBizTable}

object AShareEODPrices extends RegimeTask with TimeSeries {
  val appName: String = "AShareEODPrices"

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
    S_DQ_ADJPRECLOSE AS adj_pre_close,
    S_DQ_ADJOPEN AS adj_open,
    S_DQ_ADJHIGH AS adj_high,
    S_DQ_ADJLOW AS adj_low,
    S_DQ_ADJCLOSE AS adj_close,
    S_DQ_ADJFACTOR AS adj_factor,
    S_DQ_AVGPRICE AS average_price,
    S_DQ_TRADESTATUS AS trade_status,
    S_DQ_TRADESTATUSCODE AS trade_status_code,
    S_DQ_LIMIT AS limit_price,
    S_DQ_STOPPING AS stopping_price,
    OPDATE AS update_date
  FROM
    ASHAREEODPRICES
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  val saveTo         = "ashare_eod_prices"
  val primaryKeyName = "PK_ashare_eod_prices"
  val primaryColumn  = Seq("object_id")
  val index1         = ("IDX_ashare_eod_prices_1", Seq("update_date"))
  val index2         = ("IDX_ashare_eod_prices_2", Seq("trade_date", "symbol"))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.SyncAll :: _ =>
        syncAll(connMarket, query, connBiz, saveTo)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
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
