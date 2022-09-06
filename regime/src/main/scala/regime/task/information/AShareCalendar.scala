package regime.task.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.task.{Command, Information, RegimeTask}
import regime.task.Common.{connMarket, connBiz}

object AShareCalendar extends RegimeTask with Information {
  val appName = "AShareCalendar"

  val query = """
  SELECT
    OBJECT_ID AS object_id,
    TRADE_DAYS AS trade_days,
    S_INFO_EXCHMARKET AS exchange
  FROM
    ASHARECALENDAR
  """

  val saveTo         = "ashare_calendar"
  val primaryKeyName = "PK_ashare_calendar"
  val primaryColumn  = Seq("object_id")
  val indexName      = "IDX_ashare_calendar"
  val indexColumn    = Seq("trade_days")

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
      case _ =>
        throw new Exception("Invalid command")
    }
  }
}
