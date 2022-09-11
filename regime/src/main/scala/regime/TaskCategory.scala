package regime

import regime.market._

trait TaskCategory {
  val name = this.getClass.getSimpleName.dropRight(1)
}

object TaskCategory {
  def unapply(str: String): TaskCategory = str match {
    case Information.name => Information
    case TimeSeries.name  => TimeSeries
    case Finance.name     => Finance
    case _                => throw new Exception(s"$str is not in TaskCategory")
  }
}
