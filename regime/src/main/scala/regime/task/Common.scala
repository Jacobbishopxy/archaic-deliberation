package regime.task

import com.typesafe.config.ConfigFactory

import regime.{Global, Conn}

object Common {
  private val config        = ConfigFactory.load(Global.connConfig).getConfig("task")
  private val marketConfig  = config.getConfig("market")
  private val productConfig = config.getConfig("product")
  private val bizConfig     = config.getConfig("biz")

  val connMarket = Conn(
    marketConfig.getString("db"),
    marketConfig.getString("driver"),
    marketConfig.getString("host"),
    marketConfig.getInt("port"),
    marketConfig.getString("database"),
    marketConfig.getString("user"),
    marketConfig.getString("password")
  )

  val connProduct = Conn(
    productConfig.getString("db"),
    productConfig.getString("driver"),
    productConfig.getString("host"),
    productConfig.getInt("port"),
    productConfig.getString("database"),
    productConfig.getString("user"),
    productConfig.getString("password")
  )

  val connBiz = Conn(
    bizConfig.getString("db"),
    bizConfig.getString("driver"),
    bizConfig.getString("host"),
    bizConfig.getInt("port"),
    bizConfig.getString("database"),
    bizConfig.getString("user"),
    bizConfig.getString("password")
  )

}
