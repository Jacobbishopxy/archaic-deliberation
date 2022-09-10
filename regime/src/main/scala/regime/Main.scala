package regime

import org.apache.spark.sql.SparkSession

import regime.market.information._
import regime.market.timeseries._
import regime.market.finance._
import regime.market._

object Main extends App {
  implicit val sparkBuilder = SparkSession.builder()

  args.toList match {
    case taskCategory :: task :: commands => {
      TaskCategory.unapply(taskCategory) match {
        case Information =>
          Information.unapply(task).finish(commands: _*)
        case TimeSeries =>
          TimeSeries.unapply(task).finish(commands: _*)
        case Finance =>
          Finance.unapply(task).finish(commands: _*)
        case Consensus =>
          Consensus.unapply(task).finish(commands: _*)
      }
    }
    case _ =>
      throw new Exception(
        "Invalid arguments' format, at least three arguments are required for executing a task"
      )
  }

}
