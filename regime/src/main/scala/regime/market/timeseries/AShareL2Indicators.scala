package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, TimeSeries, RegimeTask}
import regime.market.Common.{connMarket, connBiz}

object AShareL2Indicators extends RegimeTask with TimeSeries {
  val appName: String = "AShareL2Indicators"

  val query = """
  SELECT
    OBJECT_ID as object_id,
    S_INFO_WINDCODE as symbol,
    TRADE_DT as trade_date,
    S_LI_INITIATIVEBUYRATE,
    S_LI_INITIATIVEBUYMONEY,
    S_LI_INITIATIVEBUYAMOUNT,
    S_LI_INITIATIVESELLRATE,
    S_LI_INITIATIVESELLMONEY,
    S_LI_INITIATIVESELLAMOUNT,
    S_LI_LARGEBUYRATE,
    S_LI_LARGEBUYMONEY,
    S_LI_LARGEBUYAMOUNT,
    S_LI_LARGESELLRATE,
    S_LI_LARGESELLMONEY,
    S_LI_LARGESELLAMOUNT,
    S_LI_ENTRUSTRATE,
    S_LI_ENTRUDIFFERAMOUNT,
    S_LI_ENTRUDIFFERAMONEY,
    S_LI_ENTRUSTBUYMONEY,
    S_LI_ENTRUSTSELLMONEY,
    S_LI_ENTRUSTBUYAMOUNT,
    S_LI_ENTRUSTSELLAMOUNT,
    OPDATE as update_date
  FROM
    ASHAREL2INDICATORS
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  val saveTo         = "ashare_level2_indicators"
  val primaryKeyName = "PK_ashare_level2_indicators"
  val primaryColumn  = Seq("object_id")
  val index1         = ("IDX_ashare_level2_indicators", Seq("update_date"))
  val index2         = ("IDX_ashare_level2_indicators", Seq("trade_date", "symbol"))

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
