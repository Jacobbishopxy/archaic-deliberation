package regime.market

import regime.TaskCategory
import regime.helper.RegimeSpark
import regime.market.consensus._
import regime.market.finance._
import regime.market.information._
import regime.market.timeseries._

trait Information extends RegimeSpark {
  val appName = this.getClass.getSimpleName.dropRight(1)
}

trait TimeSeries extends RegimeSpark {
  val appName = this.getClass.getSimpleName.dropRight(1)
}

trait Finance extends RegimeSpark {
  val appName = this.getClass.getSimpleName.dropRight(1)
}

trait Consensus extends RegimeSpark {
  val appName = this.getClass.getSimpleName.dropRight(1)
}

object Information extends TaskCategory {
  def unapply(str: String): Information = str match {
    case AShareCalendar.appName          => AShareCalendar
    case AShareInformationCitics.appName => AShareInformationCitics
    case AShareInformationWind.appName   => AShareInformationWind
    case AIndexInformation.appName       => AIndexInformation
    case AIndexInformationCitics.appName => AIndexInformationCitics
    case AIndexInformationWind.appName   => AIndexInformationWind
    case _                               => throw new Exception(s"$str is not in Information")
  }
}

object TimeSeries extends TaskCategory {
  def unapply(str: String): TimeSeries = str match {
    case AShareEODPrices.appName              => AShareEODPrices
    case AShareEODDerivativeIndicator.appName => AShareEODDerivativeIndicator
    case AShareYield.appName                  => AShareYield
    case AShareL2Indicator.appName            => AShareL2Indicator
    case AShareEXRightDividend.appName        => AShareEXRightDividend
    case AShareTradingSuspension.appName      => AShareTradingSuspension
    case AIndexEODPrices.appName              => AIndexEODPrices
    case AIndexEODPricesCitics.appName        => AIndexEODPricesCitics
    case AIndexEODPricesWind.appName          => AIndexEODPricesWind
    case AIndexValuation.appName              => AIndexValuation
    case AIndexFinancialDerivative.appName    => AIndexFinancialDerivative
    case _                                    => throw new Exception(s"$str is not in TimeSeries")
  }
}

object Finance extends TaskCategory {
  def unapply(str: String): Finance = str match {
    case AShareBalanceSheet.appName => AShareBalanceSheet
    case AShareCashFlow.appName     => AShareCashFlow
    case AShareIncome.appName       => AShareIncome
    // TODO
    case AShareIssuingDatePredict.appName => AShareIssuingDatePredict
    // TODO
    case AShareFinancialExpense.appName => AShareFinancialExpense
    // TODO
    case AShareProfitExpress.appName => AShareProfitExpress
    // TODO
    case AShareProfitNotice.appName => AShareProfitNotice
    // TODO
    case AShareSalesSegmentMapping.appName => AShareSalesSegmentMapping
    // TODO
    case AShareSalesSegment.appName => AShareSalesSegment
    case _                          => throw new Exception(s"$str is not in Finance")
  }
}

object Consensus extends TaskCategory {
  def unapply(str: String): Consensus = str match {
    case _ => throw new Exception(s"$str is not in Consensus")
  }
}
