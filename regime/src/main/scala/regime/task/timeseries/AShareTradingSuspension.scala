package regime.task.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.task.Common.{connMarket, connBiz}
import regime.helper._
import regime.task.{Command, TimeSeries}

object AShareTradingSuspension extends RegimeSpark with TimeSeries {
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

  def process(spark: SparkSession, args: String*): Unit = {
    args.toList match {
      case Command.SyncAll :: _                    => syncAll(spark)
      case Command.ExecuteOnce :: _                => createPrimaryKeyAndIndex()
      case Command.TimeFromUpsert :: timeFrom :: _ => upsertFromDate(spark, timeFrom)
      case _                                       => throw new Exception("Invalid command")
    }
  }

  private def syncAll(spark: SparkSession): Unit = {
    // Read from source
    val df = RegimeJdbcHelper(connMarket).readTable(spark, query)

    // Save to the target
    RegimeJdbcHelper(connBiz).saveTable(df, saveTo, SaveMode.Overwrite)
  }

  private def createPrimaryKeyAndIndex(): Unit = {
    val helper = RegimeJdbcHelper(connBiz)

    // create primary key
    helper.createPrimaryKey(saveTo, primaryKeyName, primaryColumn)

    // create index
    helper.createIndex(saveTo, indexName, indexColumn)
  }

  private def upsertFromDate(spark: SparkSession, fromDate: String): Unit = {
    // Read from source
    val df = RegimeJdbcHelper(connMarket).readTable(spark, queryFromDate(fromDate))

    // Upsert to the target
    RegimeJdbcHelper(connBiz).upsertTable(df, saveTo, None, false, primaryColumn, "doUpdate")
  }
}
