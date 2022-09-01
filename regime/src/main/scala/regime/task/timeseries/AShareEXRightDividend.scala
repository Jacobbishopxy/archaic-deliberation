package regime.task.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.SparkTaskCommon
import regime.task.Common.{connMarket, connBiz}

// TODO:
// 1. append
// 1. replace
object AShareEXRightDividend extends SparkTaskCommon {
  val appName: String = "AShareEXRightDividend ETL"

  val query = """
  ASHAREEXRIGHTDIVIDENDRECORD
  """

  def process(spark: SparkSession): Unit = {
    //
  }

}
