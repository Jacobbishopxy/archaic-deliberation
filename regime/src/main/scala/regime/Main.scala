package regime

import org.apache.spark.sql.SparkSession

import regime.market.information._
import regime.market.timeseries._
import regime.market.finance._
import regime.market._
import regime.product._
import regime.portfolio._

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
        case Product =>
          Product.unapply(task).finish(commands: _*)
        case Portfolio =>
          Portfolio.unapply(task).finish(commands: _*)
        case t @ _ =>
          throw new IllegalArgumentException("Invalid TaskCategory")
      }
    }
    case _ =>
      throw new IllegalArgumentException(
        "Invalid arguments' format, at least three arguments are required for executing a task"
      )
  }

}
