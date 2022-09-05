package regime.task

import regime.helper._
import org.apache.spark.sql.SparkSession

trait RegimeTask extends RegimeSpark {
  val appName: String

  def process(spark: SparkSession, args: String*): Unit

}
