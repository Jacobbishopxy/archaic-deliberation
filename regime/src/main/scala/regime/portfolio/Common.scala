package regime.portfolio

import com.typesafe.config.ConfigFactory

import regime._

object Common {
  private val config = ConfigFactory.load(Global.connConfig).getConfig("task")

  val conn = Conn(config.getConfig("biz"))

  def connTable(table: String): ConnTable =
    ConnTable(conn, table)

  def connTableColumn(table: String, column: String): ConnTableColumn =
    ConnTableColumn(conn, table, column)

  lazy val datetimeFormat = "yyyyMMddHHmmss"
  lazy val dateFormat     = "yyyyMMdd"

}
