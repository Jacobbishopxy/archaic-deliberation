package regime.product

import java.time.format.DateTimeFormatter
import java.time.LocalDate
import com.typesafe.config.ConfigFactory

import regime.{Global, Conn, ConnTable, ConnTableColumn}

object Common {
  private val config        = ConfigFactory.load(Global.connConfig).getConfig("task")
  private val productConfig = config.getConfig("product")
  private val bizConfig     = config.getConfig("biz")

  val connProduct = Conn(productConfig)
  val connBiz     = Conn(bizConfig)

  val timeColumnProduct = "tradeDate"
  val timeColumnBiz     = "trade_date"

  def connProductTable(table: String): ConnTable =
    ConnTable(connProduct, table)
  def connBizTable(table: String): ConnTable =
    ConnTable(connBiz, table)

  def connProductTableColumn(table: String, column: String): ConnTableColumn =
    ConnTableColumn(connProduct, table, column)
  def connBizTableColumn(table: String, column: String): ConnTableColumn =
    ConnTableColumn(connBiz, table, column)

  lazy val datetimeFormat    = "yyyyMMddHHmmss"
  lazy val dateFormat        = "yyyyMMdd"
  lazy val concatenateString = "-"
  lazy val fetchSize         = 1000000

  def convertLongLikeDatetimeToString(v: Long): String = {
    val dtFmt      = DateTimeFormatter.ofPattern(datetimeFormat)
    val parsedDate = LocalDate.parse(v.toString, dtFmt)
    val dFmt       = DateTimeFormatter.ofPattern(dateFormat)
    parsedDate.format(dFmt)
  }

  def convertStringToLongLikeDatetimeString(v: String): String =
    v + "000000"

  def convertStringToLongLikeDatetime(v: String): Long =
    v.toLong * 1000000L

}
