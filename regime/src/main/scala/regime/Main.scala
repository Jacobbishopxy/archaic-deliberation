package regime

import org.apache.spark.sql.SparkSession

import regime.task.information.AShareInformationWind
import regime.task.information.AShareInformationCitics
import regime.task.information.AShareCalendar
import regime.task.information.AShareTradingSuspension
import regime.task.timeseries.AShareEODPricesSyncAll
import regime.task.timeseries.AShareEODPricesDaily

object Main extends App {
  if (args.length < 2) {
    throw new Exception("At least two parameters are required for selecting a task")
  }

  implicit val sparkBuilder = SparkSession.builder()

  args.toList match {
    case "information" :: "AShareInformationWind" :: _ =>
      AShareInformationWind.finish()
    case "information" :: "AShareInformationCitics" :: _ =>
      AShareInformationCitics.finish()
    case "information" :: "AShareCalendar" :: _ =>
      AShareCalendar.finish()
    case "information" :: "AShareTradingSuspension" :: _ =>
      AShareTradingSuspension.finish()
    case "timeseries" :: "AShareEODPricesSyncAll" :: _ =>
      AShareEODPricesSyncAll.finish()
    case "timeseries" :: "AShareEODPricesDaily" :: tail =>
      AShareEODPricesDaily.finish(tail: _*)
    case _ => throw new Exception("Task name not found")
  }

}
