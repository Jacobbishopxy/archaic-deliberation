package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.TimeSeries
import regime.market.Common._

object AIndexEODPricesCitics extends RegimeSpark with TimeSeries {
  lazy val query = """
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
    AINDEXINDUSTRIESEODCITICS
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  lazy val readFrom       = "AINDEXINDUSTRIESEODCITICS"
  lazy val saveTo         = "aindex_eod_prices_citics"
  lazy val readUpdateCol  = "OPDATE"
  lazy val saveUpdateCol  = "update_date"
  lazy val primaryKeyName = "PK_aindex_eod_prices_citics"
  lazy val primaryColumn  = Seq("object_id")
  lazy val index1         = ("IDX_aindex_eod_prices_citics_1", Seq("update_date"))
  lazy val index2         = ("IDX_aindex_eod_prices_citics_2", Seq("trade_date", "symbol"))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(connMarket, query, connBizTable(saveTo))
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
          (primaryKeyName, primaryColumn),
          Seq(index1, index2)
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
      case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
        syncUpsert(
          connMarket,
          queryDateRange(timeFrom, timeTo),
          connBizTable(saveTo),
          primaryColumn
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
  }
}
