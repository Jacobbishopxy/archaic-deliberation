package regime

import org.apache.spark.sql.SparkSession

import regime.task.information._
import regime.task.timeseries._
import regime.task.finance._
import regime.task.Task

object Main extends App {
  if (args.length < 2) {
    throw new Exception("At least two parameters are required for selecting a task")
  }

  implicit val sparkBuilder = SparkSession.builder()

  args.toList match {
    case Task.Information :: Task.AShareInformationWind :: tail =>
      AShareInformationWind.finish(tail: _*)
    case Task.Information :: Task.AShareInformationCitics :: tail =>
      AShareInformationCitics.finish(tail: _*)
    case Task.Information :: Task.AShareCalendar :: tail =>
      AShareCalendar.finish(tail: _*)

    case Task.TimeSeries :: Task.AShareTradingSuspension :: tail =>
      AShareTradingSuspension.finish(tail: _*)
    case Task.TimeSeries :: Task.AShareEXRightDividend :: tail =>
      AShareEXRightDividend.finish(tail: _*)
    case Task.TimeSeries :: Task.AShareEODPrices :: tail =>
      AShareEODPrices.finish(tail: _*)

    case Task.Finance :: Task.AShareBalanceSheet :: tail =>
      AShareBalanceSheet.finish(tail: _*)
    case Task.Finance :: Task.AShareCashFlow :: tail =>
      AShareCashFlow.finish(tail: _*)
    case Task.Finance :: Task.AShareIncome :: tail =>
      AShareIncome.finish(tail: _*)

    case _ => throw new Exception("Task name not found")
  }

}
