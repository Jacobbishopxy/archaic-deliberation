package regime.product

import com.typesafe.config.ConfigFactory

import regime.{Global, Conn, ConnTable, ConnTableColumn}

object Common {
  private val config        = ConfigFactory.load(Global.connConfig).getConfig("task")
  private val productConfig = config.getConfig("product")
  private val bizConfig     = config.getConfig("biz")

  val connProduct = Conn(productConfig)
  val connBiz     = Conn(bizConfig)

  def connProductTable(table: String): ConnTable =
    ConnTable(connProduct, table)
  def connBizTable(table: String): ConnTable =
    ConnTable(connBiz, table)

  def connProductTableColumn(table: String, column: String): ConnTableColumn =
    ConnTableColumn(connProduct, table, column)
  def connBizTableColumn(table: String, column: String): ConnTableColumn =
    ConnTableColumn(connBiz, table, column)

}
