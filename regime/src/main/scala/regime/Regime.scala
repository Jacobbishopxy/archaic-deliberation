package regime

import org.apache.spark.sql.SparkSession

case class Conn(
    db: String,
    driver: String,
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String
) {
  def url: String = s"jdbc:$db://$host:$port/$database"

  def options: Map[String, String] = Map(
    "url"      -> this.url,
    "driver"   -> driver,
    "user"     -> user,
    "password" -> password
  )
}

trait SparkTaskCommon {
  val appName: String

  def process(spark: SparkSession): Unit

  def finish(sparkBuilder: SparkSession.Builder): Unit = {
    val spark = sparkBuilder.appName(appName).getOrCreate()
    process(spark)
    spark.stop()
  }
}

object Global {
  //
  val connConfig = "conn.conf"
}
