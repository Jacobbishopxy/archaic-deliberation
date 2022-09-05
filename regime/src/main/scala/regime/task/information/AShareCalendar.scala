package regime.task.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.SparkTaskCommon
import regime.task.Common.{connMarket, connBiz}
import regime.helper.RegimeJdbcHelper

object AShareCalendar extends SparkTaskCommon {
  val appName: String = "AShareCalendar ETL"

  val query = """
  SELECT
    OBJECT_ID as object_id,
    TRADE_DAYS as trade_days,
    S_INFO_EXCHMARKET as exchange
  FROM
    ASHARECALENDAR
  """

  val save_to = "ashare_calendar"

  def process(spark: SparkSession): Unit = {
    // Read from source
    val df = RegimeJdbcHelper(connMarket).readTable(spark, query)

    // Save to the target
    RegimeJdbcHelper(connBiz).saveTable(df, save_to, SaveMode.Overwrite)
  }
}
