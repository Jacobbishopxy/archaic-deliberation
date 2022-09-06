package regime

import org.apache.spark.sql.SparkSession

import regime.task.information._
import regime.task.timeseries._
import regime.task.finance._
import regime.task._

object Main extends App {
  implicit val sparkBuilder = SparkSession.builder()

  args.toList match {
    case taskCategory :: task :: commands => {
      TaskCategory.unapply(taskCategory).get match {
        case Information =>
          Information.unapply(task).get match {
            case t @ AShareCalendar          => t.finish(commands: _*)
            case t @ AShareInformationWind   => t.finish(commands: _*)
            case t @ AShareInformationCitics => t.finish(commands: _*)
          }
        case TimeSeries =>
          TimeSeries.unapply(task).get match {
            case t @ AShareTradingSuspension => t.finish(commands: _*)
            case t @ AShareEXRightDividend   => t.finish(commands: _*)
            case t @ AShareEODPrices         => t.finish(commands: _*)
          }
        case Finance =>
          Finance.unapply(task).get match {
            case t @ AShareIncome       => t.finish(commands: _*)
            case t @ AShareCashFlow     => t.finish(commands: _*)
            case t @ AShareBalanceSheet => t.finish(commands: _*)
          }
      }
    }
    case _ =>
      throw new Exception(
        "Invalid arguments' format, at least three arguments are required for executing a task"
      )
  }

}
