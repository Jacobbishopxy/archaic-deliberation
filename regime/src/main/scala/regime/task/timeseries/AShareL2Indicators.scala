package regime.task.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.task.{Command, TimeSeries, RegimeTask}
import regime.task.Common.{connMarket, connBiz}

object AShareL2Indicators extends RegimeTask with TimeSeries {
  val appName: String = "AShareL2Indicators"

  val query = """
  ASHAREL2INDICATORS
  """

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    //
  }

}
