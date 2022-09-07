package regime.market

import regime.market.information._
import regime.market.timeseries._
import regime.market.finance._

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

trait Consensus {
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
    case AShareCalendar.appName          => Some(AShareCalendar)
    case AShareInformationCitics.appName => Some(AShareInformationCitics)
    case AShareInformationWind.appName   => Some(AShareInformationWind)
    case AIndexInformation.appName       => Some(AIndexInformation)
    case AIndexInformationCitics.appName => Some(AIndexInformationCitics)
    case AIndexInformationWind.appName   => Some(AIndexInformationWind)
    case _                               => None
  }
}

object TimeSeries extends TaskCategory {
  val name = "TimeSeries"

  def unapply(str: String): Option[TimeSeries] = str match {
    case AShareEODPrices.appName              => Some(AShareEODPrices)
    case AShareEODDerivativeIndicator.appName => Some(AShareEODDerivativeIndicator)
    case AShareYield.appName                  => Some(AShareYield)
    case AShareL2Indicators.appName           => Some(AShareL2Indicators)
    case AShareEXRightDividend.appName        => Some(AShareEXRightDividend)
    case AShareTradingSuspension.appName      => Some(AShareTradingSuspension)
    case AIndexEODPrices.appName              => Some(AIndexEODPrices)
    case AIndexEODPricesCitics.appName        => Some(AIndexEODPricesCitics)
    case AIndexEODPricesWind.appName          => Some(AIndexEODPricesWind)
    case AIndexValuation.appName              => Some(AIndexValuation)
    case AIndexFinancialDerivative.appName    => Some(AIndexFinancialDerivative)
    case _                                    => None
  }
}

object Finance extends TaskCategory {
  val name = "Finance"

  def unapply(str: String): Option[Finance] = str match {
    case AShareBalanceSheet.appName => Some(AShareBalanceSheet)
    case AShareCashFlow.appName     => Some(AShareCashFlow)
    case AShareIncome.appName       => Some(AShareIncome)
    // TODO
    case AShareIssuingDatePredict.appName => Some(AShareIssuingDatePredict)
    // TODO
    case AShareFinancialExpense.appName => Some(AShareFinancialExpense)
    // TODO
    case AShareProfitExpress.appName => Some(AShareProfitExpress)
    // TODO
    case AShareProfitNotice.appName => Some(AShareProfitNotice)
    // TODO
    case AShareSalesSegmentMapping.appName => Some(AShareSalesSegmentMapping)
    // TODO
    case AShareSalesSegment.appName => Some(AShareSalesSegment)
    case _                          => None
  }
}

object Consensus extends TaskCategory {
  val name: String = "Consensus"

  def unapply(str: String): Option[Consensus] = str match {
    case _ => None
  }
}
