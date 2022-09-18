package regime.market.finance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Finance
import regime.market.Common.{connMarket, connBiz}

object AShareProfitExpress extends Finance {
  val query = """
  ASHAREPROFITEXPRESS
  """

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    //
  }

}
