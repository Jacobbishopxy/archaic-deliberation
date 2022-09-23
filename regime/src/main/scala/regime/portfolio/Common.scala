package regime.portfolio

import com.typesafe.config.ConfigFactory

import regime._

object Common {
  private val config = ConfigFactory.load(Global.connConfig).getConfig("task")

  val conn = Conn(config.getConfig("biz"))

  object Token {
    val tradeDate  = "trade_date"
    val symbol     = "symbol"
    val productNum = "product_num"
  }

  def connTable(table: String): ConnTable =
    ConnTable(conn, table)

  def connTableColumn(table: String, column: String): ConnTableColumn =
    ConnTableColumn(conn, table, column)

  lazy val datetimeFormat         = "yyyy-MM-dd HH:mm:ss"
  lazy val dateFormat             = "yyyyMMdd"
  lazy val numOfTradeDateInAMouth = 20
  lazy val numOfTradeDateInAYear  = 250

}
