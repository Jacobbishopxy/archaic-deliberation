package regime.task

import regime.task.information._
import regime.task.timeseries._
import regime.task.finance._

sealed trait TaskCategory {
  val name: String
}

trait Information {
  val appName: String
}

trait TimeSeries {
  val appName: String
}

trait Finance {
  val appName: String
}

object TaskCategory {
  def unapply(str: String): Option[TaskCategory] = str match {
    case Information.name => Some(Information)
    case TimeSeries.name  => Some(TimeSeries)
    case Finance.name     => Some(Finance)
    case _                => None
  }
}

object Information extends TaskCategory {
  val name = "Information"

  def unapply(str: String): Option[Information] = str match {
    case AShareInformationCitics.appName => Some(AShareInformationCitics)
    case AShareInformationWind.appName   => Some(AShareInformationWind)
    case AShareCalendar.appName          => Some(AShareCalendar)
    case _                               => None
  }
}

object TimeSeries extends TaskCategory {
  val name = "TimeSeries"

  def unapply(str: String): Option[TimeSeries] = str match {
    case AShareEODPrices.appName         => Some(AShareEODPrices)
    case AShareEXRightDividend.appName   => Some(AShareEXRightDividend)
    case AShareTradingSuspension.appName => Some(AShareTradingSuspension)
    case _                               => None
  }
}

object Finance extends TaskCategory {
  val name = "Finance"

  def unapply(str: String): Option[Finance] = str match {
    case AShareBalanceSheet.appName => Some(AShareBalanceSheet)
    case AShareCashFlow.appName     => Some(AShareCashFlow)
    case AShareIncome.appName       => Some(AShareIncome)
    case _                          => None
  }
}
