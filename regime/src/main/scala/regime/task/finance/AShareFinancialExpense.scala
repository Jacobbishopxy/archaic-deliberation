package regime.task.finance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.task.{Command, Finance, RegimeTask}
import regime.task.Common.{connMarket, connBiz}

object AShareFinancialExpense extends RegimeTask with Finance {
  val appName: String = "AShareFinancialExpense"

  val query = """
  ASHAREFINANCIALEXPENSE
  """

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    //
  }

}
