package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, TimeSeries, RegimeTask}
import regime.market.Common.{connMarket, connBizTable}

object AShareTradingSuspension extends RegimeTask with TimeSeries {
  val appName: String = "AShareTradingSuspension"

  val query = """
  SELECT
    OBJECT_ID as object_id,
    S_INFO_WINDCODE AS symbol,
    S_DQ_SUSPENDDATE AS suspend_date,
    S_DQ_SUSPENDTYPE AS suspend_type,
    S_DQ_RESUMEPDATE AS resume_date,
    S_DQ_CHANGEREASON AS reason,
    S_DQ_TIME AS suspend_time,
    OPDATE AS update_date
  FROM
    ASHARETRADINGSUSPENSION
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  val saveTo         = "ashare_trading_suspension"
  val primaryKeyName = "PK_ashare_trading_suspension"
  val primaryColumn  = Seq("object_id")
  val index          = ("IDX_ashare_trading_suspension", Seq("update_date"))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.SyncAll :: _ =>
        syncAll(connMarket, query, connBizTable(saveTo))
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
          (primaryKeyName, primaryColumn),
          Seq(index)
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          connMarket,
          queryFromDate(timeFrom),
          connBizTable(saveTo),
          primaryColumn
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
  }
}
