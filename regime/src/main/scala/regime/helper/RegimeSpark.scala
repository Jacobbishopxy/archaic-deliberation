package regime.helper

import org.apache.spark.sql.SparkSession

trait RegimeSpark {
  val appName: String

  def process(args: String*)(implicit spark: SparkSession): Unit

  def initialize()(implicit spark: SparkSession): Unit = {
    // Not every Spark task needs this method
  }

  def finish(args: String*)(implicit sparkBuilder: SparkSession.Builder): Unit = {
    implicit val spark = sparkBuilder.appName(appName).getOrCreate()
    process(args: _*)
    spark.stop()
  }
}
