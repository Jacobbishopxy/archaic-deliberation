package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, TimeSeries, RegimeTask}
import regime.market.Common._

object AIndexValuation extends RegimeTask with TimeSeries {
  lazy val query = """
  SELECT
    OBJECT_ID AS object_id,
    S_INFO_WINDCODE AS symbol,
    TRADE_DT AS trade_date,
    CON_NUM,
    PE_LYR,
    PE_TTM,
    PB_LF,
    PCF_LYR,
    PCF_TTM,
    PS_LYR,
    PS_TTM,
    MV_TOTAL,
    MV_FLOAT,
    DIVIDEND_YIELD,
    PEG_HIS,
    TOT_SHR,
    TOT_SHR_FLOAT,
    TOT_SHR_FREE,
    TURNOVER,
    TURNOVER_FREE,
    EST_NET_PROFIT_Y1,
    EST_NET_PROFIT_Y2,
    EST_BUS_INC_Y1,
    EST_BUS_INC_Y2,
    EST_EPS_Y1,
    EST_EPS_Y2,
    EST_YOYPROFIT_Y1,
    EST_YOYPROFIT_Y2,
    EST_YOYGR_Y1,
    EST_YOYGR_Y2,
    EST_PE_Y1,
    EST_PE_Y2,
    EST_PEG_Y1,
    EST_PEG_Y2,
    OPDATE AS update_date
  FROM
    AINDEXVALUATION
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  lazy val readFrom       = "AINDEXVALUATION"
  lazy val saveTo         = "aindex_valuation"
  lazy val readUpdateCol  = "OPDATE"
  lazy val saveUpdateCol  = "update_date"
  lazy val primaryKeyName = "PK_aindex_valuation"
  lazy val primaryColumn  = Seq("object_id")
  lazy val index1         = ("IDX_aindex_valuation_1", Seq("update_date"))
  lazy val index2         = ("IDX_aindex_valuation_2", Seq("trade_date", "symbol"))

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
