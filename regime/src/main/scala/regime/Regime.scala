package regime

import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import scala.collection.JavaConverters._

case class Conn(
    db: String,
    driver: String,
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String,
    databaseOptions: String,
    extraOptions: Map[String, String]
) {

  lazy val driverType = driver match {
    case "com.microsoft.sqlserver.jdbc.SQLServerDriver" =>
      DriverType.MsSql
    case "com.mysql.cj.jdbc.Driver" =>
      DriverType.MySql
    case "org.postgresql.Driver" =>
      DriverType.Postgres
    case _ =>
      DriverType.Other
  }

  def url: String = {
    driverType match {
      case DriverType.MsSql =>
        s"jdbc:$db://$host:$port;databaseName=$database;encrypt=true;trustServerCertificate=true;"
      case _ =>
        // s"jdbc:$db://$host:$port/$database?useUnicode=true&characterEncoding=UTF-8"
        // s"jdbc:$db://$host:$port/$database?useUnicode=true&characterEncoding=GBK"
        s"jdbc:$db://$host:$port/$database?$databaseOptions"
    }
  }

  def options = Map(
    "url"      -> this.url,
    "driver"   -> driver,
    "user"     -> user,
    "password" -> password
  ) ++ extraOptions

  def optionsAppend(newOptions: Map[String, String]): Conn =
    this.copy(extraOptions = this.extraOptions ++ newOptions)

}

object Conn {
  def apply(config: Config): Conn = {

    val eo = config
      .getObject("extraOptions")
      .entrySet()
      .asScala
      .map(entry => (entry.getKey, entry.getValue().render()))
      .toMap

    Conn(
      config.getString("db"),
      config.getString("driver"),
      config.getString("host"),
      config.getInt("port"),
      config.getString("database"),
      config.getString("user"),
      config.getString("password"),
      config.getString("databaseOptions"),
      eo
    )
  }
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
