package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, TimeSeries, RegimeTask}
import regime.market.Common.{connMarket, connBiz}

object AIndexValuation extends RegimeTask with TimeSeries {
  val appName: String = "AIndexValuation"

  val query = """
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

  val saveTo         = "aindex_valuation"
  val primaryKeyName = "PK_aindex_valuation"
  val primaryColumn  = Seq("object_id")
  val index1         = ("IDX_aindex_valuation_1", Seq("update_date"))
  val index2         = ("IDX_aindex_valuation_2", Seq("trade_date", "symbol"))

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
