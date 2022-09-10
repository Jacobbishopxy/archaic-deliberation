package regime

import regime.market._
import regime.market.information._
import regime.market.timeseries._
import regime.market.finance._

object TestTaskCategory extends App {
  val mockArgs = Seq("Information", "AShareInformationCitics", "SyncAll")

  mockArgs match {
    case taskCategory :: task :: commands => {
      TaskCategory.unapply(taskCategory) match {
        case Information =>
          Information.unapply(task) match {
            case t @ AShareCalendar          => println(s"${t.appName} do $commands")
            case t @ AShareInformationWind   => println(s"${t.appName} do $commands")
            case t @ AShareInformationCitics => println(s"${t.appName} do $commands")
          }
        case TimeSeries =>
          TimeSeries.unapply(task) match {
            case t @ AShareTradingSuspension => println(s"${t.appName} do $commands")
            case t @ AShareEXRightDividend   => println(s"${t.appName} do $commands")
            case t @ AShareEODPrices         => println(s"${t.appName} do $commands")
          }
        case Finance =>
          Finance.unapply(task) match {
            case t @ AShareIncome       => println(s"${t.appName} do $commands")
            case t @ AShareCashFlow     => println(s"${t.appName} do $commands")
            case t @ AShareBalanceSheet => println(s"${t.appName} do $commands")
          }
        case Consensus =>
          Consensus.unapply(task) match {
            case _ => {}
          }
      }
    }
    case _ => throw new Exception("Invalid arguments' format")
  }
}
