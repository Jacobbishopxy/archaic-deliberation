package regime

import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config

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

  def driverType: DriverType.Value = driver match {
    case "com.microsoft.sqlserver.Driver" => DriverType.MsSql
    case "com.mysql.jdbc.Driver"          => DriverType.MySql
    case "org.postgresql.Driver"          => DriverType.Postgres
    case _                                => DriverType.Other
  }
}

object Conn {
  def apply(
      db: String,
      driver: String,
      host: String,
      port: Int,
      database: String,
      user: String,
      password: String
  ): Conn =
    Conn(db, driver, host, port, database, user, password)

  def apply(config: Config): Conn =
    Conn(
      config.getString("db"),
      config.getString("driver"),
      config.getString("host"),
      config.getInt("port"),
      config.getString("database"),
      config.getString("user"),
      config.getString("password")
    )
}

object DriverType extends Enumeration {
  type DriverType = Value
  val Postgres, MySql, MsSql, Other = Value
}

trait SparkTaskCommon {
  val appName: String

  def process(spark: SparkSession): Unit

  def finish(args: String*)(implicit sparkBuilder: SparkSession.Builder): Unit = {
    val spark = sparkBuilder.appName(appName).getOrCreate()
    process(spark)
    spark.stop()
  }
}

object Global {
  //
  val connConfig = "conn.conf"
}
