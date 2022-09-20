package regime.portfolio.helper

import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._

import regime.Conn
import regime.helper._
import regime.portfolio.Common._

class CalendarHelper(helper: RegimeJdbcHelper)(implicit spark: SparkSession) {
  import spark.implicits._

  lazy val tv = "CALENDAR"
  lazy val calendar = helper.readTable(
    RegimeSqlHelper.fromResource("sql/portfolio/retrieve_calendar.sql")
  )

  def findTradingDaysInThePastNNaturalDays(n: Int): Seq[Date] = {
    if (n <= 0) throw new IllegalArgumentException("N should be a positive integer")
    calendar.createOrReplaceTempView(tv)

    val pnd = s"""
    SELECT * FROM $tv WHERE ${Token.tradeDate} >= (SELECT date_sub(current_date(), $n))
    """

    spark.sql(pnd).select(Token.tradeDate).map(_.getDate(0)).collect.toSeq
  }

  def findNearestTradingDayFromThePastNNaturalDays(n: Int): Date = {
    if (n <= 0) throw new IllegalArgumentException("N should be a positive integer")
    calendar.createOrReplaceTempView(tv)

    val pnd = s"""
    SELECT first_value(*) FROM $tv WHERE ${Token.tradeDate} >= (SELECT date_sub(current_date(), $n))
    """

    spark.sql(pnd).select(Token.tradeDate).head().getDate(0)
  }

}
