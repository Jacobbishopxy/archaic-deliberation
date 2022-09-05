package regime.task

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

object Task {

  val Information             = "Information"
  val AShareInformationWind   = "AShareInformationWind"
  val AShareInformationCitics = "AShareInformationCitics"
  val AShareCalendar          = "AShareCalendar"

  val TimeSeries              = "TimeSeries"
  val AShareTradingSuspension = "AShareTradingSuspension"
  val AShareEXRightDividend   = "AShareEXRightDividend"
  val AShareEODPrices         = "AShareEODPrices"

  val Finance            = "Finance"
  val AShareBalanceSheet = "AShareBalanceSheet"
  val AShareCashFlow     = "AShareCashFlow"
  val AShareIncome       = "AShareIncome"

}

object Command {
  val SyncAll         = "SyncAll"
  val DailyUpsert     = "DailyUpsert"
  val DailyDelete     = "DailyDelete"
  val TimeFromUpsert  = "TimeFromUpsert"
  val TimeFromDelete  = "TimeFromDelete"
  val TimeRangeUpsert = "TimeRangeUpsert"
  val TimeRangeDelete = "TimeRangeDelete"
  val Initialize      = "Initialize"
  val ExecuteOnce     = "ExecuteOnce"
}
