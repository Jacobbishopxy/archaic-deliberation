package regime.market

import com.typesafe.config.ConfigFactory

import regime.{Global, Conn}

object Common {
  private val config        = ConfigFactory.load(Global.connConfig).getConfig("task")
  private val marketConfig  = config.getConfig("market")
  private val productConfig = config.getConfig("product")
  private val bizConfig     = config.getConfig("biz")

  val connMarket  = Conn(marketConfig)
  val connProduct = Conn(productConfig)
  val connBiz     = Conn(bizConfig)
}
