package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, TimeSeries, RegimeTask}
import regime.market.Common.{connMarket, connBizTable}

object AShareL2Indicator extends RegimeTask with TimeSeries {
  val appName: String = "AShareL2Indicator"

  val query = """
  SELECT
    OBJECT_ID AS object_id,
    S_INFO_WINDCODE AS symbol,
    TRADE_DT AS trade_date,
    S_LI_INITIATIVEBUYRATE AS initiative_buy_rate,
    S_LI_INITIATIVEBUYMONEY AS initiative_buy_money,
    S_LI_INITIATIVEBUYAMOUNT AS initiative_buy_amount,
    S_LI_INITIATIVESELLRATE AS initiative_sell_rate,
    S_LI_INITIATIVESELLMONEY AS initiative_sell_money,
    S_LI_INITIATIVESELLAMOUNT AS initiative_sell_amount,
    S_LI_LARGEBUYRATE AS large_buy_rate,
    S_LI_LARGEBUYMONEY AS large_buy_money,
    S_LI_LARGEBUYAMOUNT AS large_buy_amount,
    S_LI_LARGESELLRATE AS large_sell_rate,
    S_LI_LARGESELLMONEY AS large_sell_money,
    S_LI_LARGESELLAMOUNT AS large_sell_amount,
    S_LI_ENTRUSTRATE AS entrust_rate,
    S_LI_ENTRUDIFFERAMOUNT AS entrust_differ_amount,
    S_LI_ENTRUDIFFERAMONEY AS entrust_differ_amoney,
    S_LI_ENTRUSTBUYMONEY AS entrust_buy_money,
    S_LI_ENTRUSTSELLMONEY AS entrust_sell_money,
    S_LI_ENTRUSTBUYAMOUNT AS entrust_buy_amount,
    S_LI_ENTRUSTSELLAMOUNT AS entrust_sell_amount,
    OPDATE AS update_date
  FROM
    ASHAREL2INDICATORS
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  val saveTo         = "ashare_level2_indicator"
  val primaryKeyName = "PK_ashare_level2_indicator"
  val primaryColumn  = Seq("object_id")
  val index1         = ("IDX_ashare_level2_indicator1", Seq("update_date"))
  val index2         = ("IDX_ashare_level2_indicator2", Seq("trade_date", "symbol"))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.SyncAll :: _ =>
        syncAll(connMarket, query, connBizTable(saveTo))
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
