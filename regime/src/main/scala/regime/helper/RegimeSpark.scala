package regime.helper

import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import org.apache.log4j.Level

trait RegimeSpark {
  val appName: String

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def process(args: String*)(implicit spark: SparkSession): Unit

  def initialize()(implicit spark: SparkSession): Unit = {
    // Not every Spark task needs this method
  }

  def finish(args: String*)(implicit sparkBuilder: SparkSession.Builder): Unit = {
    log.info(s"args: $args")
    implicit val spark = sparkBuilder.appName(appName).getOrCreate()
    process(args: _*)
    spark.stop()
  }
}
