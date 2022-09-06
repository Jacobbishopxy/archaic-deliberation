package regime

import regime.task._
import regime.task.information._
import regime.task.timeseries._
import regime.task.finance._

object TestTaskCategory extends App {
  val mockArgs = Seq("Information", "AShareCalendar", "syncAll")

  mockArgs match {
    case taskCategory :: task :: commands => {
      TaskCategory.unapply(taskCategory).get match {
        case Information =>
          Information.unapply(task).get match {
            case t @ AShareCalendar          => println(s"${t.appName} do $commands")
            case t @ AShareInformationWind   => println(s"${t.appName} do $commands")
            case t @ AShareInformationCitics => println(s"${t.appName} do $commands")
          }
        case TimeSeries =>
          TimeSeries.unapply(task).get match {
            case t @ AShareTradingSuspension => println(s"${t.appName} do $commands")
            case t @ AShareEXRightDividend   => println(s"${t.appName} do $commands")
            case t @ AShareEODPrices         => println(s"${t.appName} do $commands")
          }
        case Finance =>
          Finance.unapply(task).get match {
            case t @ AShareIncome       => println(s"${t.appName} do $commands")
            case t @ AShareCashFlow     => println(s"${t.appName} do $commands")
            case t @ AShareBalanceSheet => println(s"${t.appName} do $commands")
          }
        case Consensus =>
          Consensus.unapply(task).get match {
            case _ => {}
          }
      }
    }
    case _ => throw new Exception("Invalid arguments' format")
  }
}
