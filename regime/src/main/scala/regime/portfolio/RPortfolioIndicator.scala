package regime.portfolio

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import regime.helper._
import regime.portfolio.helper._
import regime.portfolio.Common._
import org.apache.spark.sql.SaveMode

/** Portfolio Indicators
  *
  * Volatility: rolling_window
  *
  * DownwardVolatility: rolling_window & filter
  *
  * MaxDrawback & MaxDrawback days count
  *
  * SharpeRatio
  *
  * SortinoRatio
  *
  * CalmarRatio
  */

object RPortfolioIndicator extends Portfolio {

  lazy val query = RegimeSqlHelper.fromResource("sql/portfolio/RPortfolioNetValueChange.sql")

  override def process(args: String*)(implicit spark: SparkSession): Unit =
    args.toList match {
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }

}
