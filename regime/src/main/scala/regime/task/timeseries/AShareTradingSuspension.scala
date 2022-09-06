package regime.task.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.task.{Command, TimeSeries, RegimeTask}
import regime.task.Common.{connMarket, connBiz}

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
  val indexName      = "IDX_ashare_trading_suspension"
  val indexColumn    = Seq("update_date")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.SyncAll :: _ =>
        syncAll(connMarket, query, connBiz, saveTo)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBiz,
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq((indexName, indexColumn))
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          connMarket,
          queryFromDate(timeFrom),
          connBiz,
          primaryColumn,
          saveTo
        )
      case _ =>
        throw new Exception("Invalid command")
    }
  }
}
