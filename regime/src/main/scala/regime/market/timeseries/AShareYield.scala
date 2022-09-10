package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, TimeSeries, RegimeTask}
import regime.market.Common.{connMarket, connBiz, connBizTable}

object AShareYield extends RegimeTask with TimeSeries {
  val appName: String = "AShareYield"

  val query = """
  SELECT
    OBJECT_ID AS object_id,
    S_INFO_WINDCODE AS symbol,
    TRADE_DT AS trade_date,
    PCT_CHANGE_D AS pct_change_d,
    PCT_CHANGE_W AS pct_change_w,
    PCT_CHANGE_M AS pct_change_m,
    VOLUME_W AS volume_w,
    VOLUME_M AS volume_m,
    AMOUNT_W AS amount_w,
    AMOUNT_M AS amount_m,
    TURNOVER_D AS turnover_d,
    TURNOVER_D_FLOAT AS turnover_d_float,
    TURNOVER_W AS turnover_w,
    TURNOVER_W_FLOAT AS turnover_w_float,
    TURNOVER_W_AVE AS turnover_w_ave,
    TURNOVER_W_AVE_FLOAT AS turnover_w_ave_float,
    TURNOVER_M AS turnover_m,
    TURNOVER_M_FLOAT AS turnover_m_float,
    TURNOVER_M_AVE AS turnover_m_ave,
    TURNOVER_M_AVE_FLOAT AS turnover_m_ave_float,
    PCT_CHANGE_AVE_100W AS pct_change_ave_100w,
    STD_DEVIATION_100W AS std_deviation_100w,
    VARIANCE_100W AS variance_100w,
    PCT_CHANGE_AVE_24M AS pct_change_ave_24m,
    STD_DEVIATION_24M AS std_deviation_24m,
    VARIANCE_24M AS variance_24m,
    PCT_CHANGE_AVE_60M AS pct_change_ave_60m,
    STD_DEVIATION_60M AS std_deviation_60m,
    VARIANCE_60M AS variance_60m,
    BETA_DAY_1Y AS beta_day_1y,
    BETA_DAY_2Y AS beta_day_2y,
    ALPHA_DAY_1Y AS alpha_day_1y,
    ALPHA_DAY_2Y AS alpha_day_2y,
    BETA_100W AS beta_100w,
    ALPHA_100W AS alpha_100w,
    BETA_24M AS beta_24m,
    BETA_60M AS beta_60m,
    ALPHA_24M AS alpha_24m,
    ALPHA_60M AS alpha_60m,
    OPDATE AS update_date
  FROM
    ASHAREYIELD
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  val saveTo         = "ashare_yield"
  val primaryKeyName = "PK_ashare_yield"
  val primaryColumn  = Seq("object_id")
  val index1         = ("IDX_ashare_yield_1", Seq("update_date"))
  val index2         = ("IDX_ashare_yield_2", Seq("trade_date", "symbol"))

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
