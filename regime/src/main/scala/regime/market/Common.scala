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

object Command {
  val Initialize            = "Initialize"
  val SyncAll               = "SyncAll"
  val DailyUpsert           = "DailyUpsert"
  val DailyDelete           = "DailyDelete"
  val TimeFromTillNowUpsert = "TimeFromTillNowUpsert"
  val TimeFromTillNowDelete = "TimeFromTillNowDelete"
  val TimeRangeUpsert       = "TimeRangeUpsert"
  val TimeRangeDelete       = "TimeRangeDelete"
  val ExecuteOnce           = "ExecuteOnce"
}
