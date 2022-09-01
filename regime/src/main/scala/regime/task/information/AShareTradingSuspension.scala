package regime.task.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.SparkTaskCommon
import regime.task.Common.{connMarket, connBiz}

object AShareTradingSuspension extends SparkTaskCommon {
  val appName: String = "AShareTradingSuspension ETL"

  val query = """
  SELECT
    S_INFO_WINDCODE as symbol,
    S_DQ_SUSPENDDATE as suspend_date,
    S_DQ_SUSPENDTYPE as suspend_type,
    S_DQ_RESUMEPDATE as resume_date,
    S_DQ_CHANGEREASON as reason,
    S_DQ_TIME as suspend_time
  FROM
    ASHARETRADINGSUSPENSION
  """

  val save_to = "ashare_trading_suspension"

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
