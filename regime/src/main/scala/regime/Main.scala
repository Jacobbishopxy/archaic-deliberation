package regime

import org.apache.spark.sql.SparkSession

import regime.task.information.AShareInformationWind
import regime.task.information.AShareInformationCitics
import regime.task.information.AShareCalendar
import regime.task.timeseries.AShareTradingSuspension
import regime.task.timeseries.AShareEXRightDividend
import regime.task.timeseries.AShareEODPrices

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

    // TODO:
    case Task.TimeSeries :: Task.AShareTradingSuspension :: tail =>
      AShareTradingSuspension.finish(tail: _*)
    case Task.TimeSeries :: Task.AShareEXRightDividend :: tail =>
      AShareEXRightDividend.finish(tail: _*)
    case Task.TimeSeries :: Task.AShareEODPrices :: tail =>
      AShareEODPrices.finish(tail: _*)

    // TODO:
    case Task.Finance :: Task.AShareBalanceSheet :: tail => {}
    case Task.Finance :: Task.AShareCashFlow :: tail     => {}
    case Task.Finance :: Task.AShareIncome :: tail       => {}

    case _ => throw new Exception("Task name not found")
  }

}
