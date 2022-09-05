package regime

import regime.task._
import regime.task.information._
import regime.task.timeseries._
import regime.task.finance._

object TestTaskCategory extends App {
  val mockArgs = Seq("Information", "AShareCalendar")

  TaskCategory
    .unapply(mockArgs(0))
    .map(t =>
      t match {
        case st @ Information =>
          st.unapply(mockArgs(1))
            .map(a =>
              a match {
                case AShareInformationCitics => {}
                case AShareInformationWind   => {}
                case AShareCalendar          => {}
              }
            )
        case st @ TimeSeries =>
          st.unapply(mockArgs(1))
            .map(a =>
              a match {
                case AShareEODPrices         => {}
                case AShareEXRightDividend   => {}
                case AShareTradingSuspension => {}
              }
            )
        case st @ Finance =>
          st.unapply(mockArgs(1))
            .map(a =>
              a match {
                case AShareBalanceSheet => {}
                case AShareCashFlow     => {}
                case AShareIncome       => {}
              }
            )
      }
    )
}
