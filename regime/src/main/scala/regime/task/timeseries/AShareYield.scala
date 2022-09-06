package regime.task.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.task.{Command, TimeSeries, RegimeTask}
import regime.task.Common.{connMarket, connBiz}

object AShareYield extends RegimeTask with TimeSeries {
  val appName: String = "AShareYield"

  val query = """
  SELECT
    OBJECT_ID AS object_id,
    S_INFO_WINDCODE AS symbol,
    TRADE_DT AS trade_date,
    PCT_CHANGE_D,
    PCT_CHANGE_W,
    PCT_CHANGE_M,
    VOLUME_W,
    VOLUME_M,
    AMOUNT_W,
    AMOUNT_M,
    TURNOVER_D,
    TURNOVER_D_FLOAT,
    TURNOVER_W,
    TURNOVER_W_FLOAT,
    TURNOVER_W_AVE,
    TURNOVER_W_AVE_FLOAT,
    TURNOVER_M,
    TURNOVER_M_FLOAT,
    TURNOVER_M_AVE,
    TURNOVER_M_AVE_FLOAT,
    PCT_CHANGE_AVE_100W,
    STD_DEVIATION_100W,
    VARIANCE_100W,
    PCT_CHANGE_AVE_24M,
    STD_DEVIATION_24M,
    VARIANCE_24M,
    PCT_CHANGE_AVE_60M,
    STD_DEVIATION_60M,
    VARIANCE_60M,
    BETA_DAY_1Y,
    BETA_DAY_2Y,
    ALPHA_DAY_1Y,
    ALPHA_DAY_2Y,
    BETA_100W,
    ALPHA_100W,
    BETA_24M,
    BETA_60M,
    ALPHA_24M,
    ALPHA_60M,
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
      case _ => throw new Exception("Invalid command")
    }
  }

}
