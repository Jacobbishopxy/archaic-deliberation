package regime.task.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.task.{Command, TimeSeries, RegimeTask}
import regime.task.Common.{connMarket, connBiz}

object AShareEODDerivativeIndicator extends RegimeTask with TimeSeries {
  val appName: String = "AShareEODDerivativeIndicator"

  val query = """
  ASHAREEODDERIVATIVEINDICATOR
  """

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    //
  }

}
