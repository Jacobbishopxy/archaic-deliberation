package regime.task.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.task.Common.{connMarket, connBiz}
import regime.helper._
import regime.task.{Command, Information}

object AShareCalendar extends RegimeSpark with Information {
  val appName = "AShareCalendar"

  val query = """
  SELECT
    OBJECT_ID as object_id,
    TRADE_DAYS as trade_days,
    S_INFO_EXCHMARKET as exchange
  FROM
    ASHARECALENDAR
  """

  val saveTo         = "ashare_calendar"
  val primaryKeyName = "PK_ashare_calendar"
  val primaryColumn  = Seq("object_id")
  val indexName      = "IDX_ashare_calendar"
  val indexColumn    = Seq("trade_days")

  def process(spark: SparkSession, args: String*): Unit = {
    args.toList match {
      case Command.SyncAll :: _     => syncAll(spark)
      case Command.ExecuteOnce :: _ => createPrimaryKeyAndIndex()
      case _                        => throw new Exception("Invalid command")
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

    // primary key
    helper.createPrimaryKey(saveTo, primaryKeyName, primaryColumn)

    // index
    helper.createIndex(saveTo, indexName, indexColumn)
  }
}
