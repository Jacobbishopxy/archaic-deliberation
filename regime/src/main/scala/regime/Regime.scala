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

  lazy val driverType = driver match {
    case "com.microsoft.sqlserver.jdbc.SQLServerDriver" => DriverType.MsSql
    case "com.mysql.jdbc.Driver"                        => DriverType.MySql
    case "org.postgresql.Driver"                        => DriverType.Postgres
    case _                                              => DriverType.Other
  }

  def url: String = {
    driverType match {
      case DriverType.MsSql =>
        s"jdbc:$db://$host:$port;databaseName=$database;encrypt=true;trustServerCertificate=true;"
      case _ =>
        s"jdbc:$db://$host:$port/$database"
    }
  }

  def options = Map(
    "url"      -> this.url,
    "driver"   -> driver,
    "user"     -> user,
    "password" -> password
  )
}

object Conn {
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

case class ConnTable(conn: Conn, table: String) {
  def options = conn.options + ("dbtable" -> table)
}

case class ConnTableColumn(conn: Conn, table: String, column: String) {
  def options = conn.options + ("dbtable" -> table)
}

object Global {
  // Secret configs: Database connection information
  val connConfig = "conn.conf"
}
