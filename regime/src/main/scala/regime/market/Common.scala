package regime.market

import com.typesafe.config.ConfigFactory

import regime.{Global, Conn, ConnTable}
import regime.ConnTableColumn

object Common {
  private val config        = ConfigFactory.load(Global.connConfig).getConfig("task")
  private val marketConfig  = config.getConfig("market")
  private val productConfig = config.getConfig("product")
  private val bizConfig     = config.getConfig("biz")

  val connMarket  = Conn(marketConfig)
  val connProduct = Conn(productConfig)
  val connBiz     = Conn(bizConfig)

  def connMarketTable(table: String): ConnTable =
    ConnTable(connMarket, table)
  def connProductTable(table: String): ConnTable =
    ConnTable(connProduct, table)
  def connBizTable(table: String): ConnTable =
    ConnTable(connBiz, table)

  def connMarketTableColumn(table: String, column: String): ConnTableColumn =
    ConnTableColumn(connMarket, table, column)
  def connProductTableColumn(table: String, column: String): ConnTableColumn =
    ConnTableColumn(connProduct, table, column)
  def connBizTableColumn(table: String, column: String): ConnTableColumn =
    ConnTableColumn(connBiz, table, column)

}
