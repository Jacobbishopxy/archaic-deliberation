package regime.market.finance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, Finance, RegimeTask}
import regime.market.Common.{connMarket, connBiz}

object AShareProfitNotice extends RegimeTask with Finance {
  val appName: String = "AShareProfitNotice"

  val query = """
  ASHAREPROFITNOTICE
  """

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    //
  }

}
