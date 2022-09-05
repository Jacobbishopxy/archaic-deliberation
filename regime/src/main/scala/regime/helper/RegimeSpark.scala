package regime.helper

import org.apache.spark.sql.SparkSession

trait RegimeSpark {
  val appName: String

  def process(spark: SparkSession, args: String*): Unit

  def initialize(spark: SparkSession): Unit = {
    // Not every Spark task needs this method
  }

  def finish(args: String*)(implicit sparkBuilder: SparkSession.Builder): Unit = {
    val spark = sparkBuilder.appName(appName).getOrCreate()
    process(spark, args: _*)
    spark.stop()
  }
}
