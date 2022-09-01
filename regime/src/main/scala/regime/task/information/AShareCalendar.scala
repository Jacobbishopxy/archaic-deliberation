package regime.task.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.SparkTaskCommon
import regime.task.Common.{connMarket, connBiz}

object AShareCalendar extends SparkTaskCommon {
  val appName: String = "AShareCalendar ETL"

  val query = """
  SELECT
    TRADE_DAYS as trade_days,
    S_INFO_EXCHMARKET as exchange
  FROM
    ASHARECALENDAR
  """

  val save_to = "ashare_calendar"

  def process(spark: SparkSession): Unit = {
    // Read from source
    val df = spark.read
      .format("jdbc")
      .options(connMarket.options)
      .option("query", query)
      .load()

    // Save to the target
    df.write
      .format("jdbc")
      .options(connBiz.options)
      .option("dbtable", save_to)
      .mode(SaveMode.Overwrite)
      .save()
  }

}
