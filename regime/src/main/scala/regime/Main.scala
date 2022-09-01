package regime

import org.apache.spark.sql.SparkSession

import regime.task.info.AShareInformationWind
import regime.task.info.AShareInformationCitics

object Main extends App {
  if (args.length < 2) {
    throw new Exception("At least two parameters are required for selecting a task")
  }

  val sparkBuilder = SparkSession.builder()

  args(0) match {
    case "info" => {
      args(1) match {
        case "AShareInformationWind" => {
          AShareInformationWind.finish(sparkBuilder)
        }
        case "AShareInformationCitics" => {
          AShareInformationCitics.finish(sparkBuilder)
        }
        case _ => throw new Exception("Sub task not found")
      }
    }
    case _ => throw new Exception("Task name not found")
  }

}
