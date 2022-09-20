package regime.portfolio

import org.apache.spark.sql.SparkSession

import regime.helper._
import regime.portfolio.helper._

/** Indicators collection
  *
  *   1. AnnualizedRoteOfReturn
  */
object RPortfolioIndicators extends Portfolio {

  lazy val rawQuery = RegimeSqlHelper.fromResource("sql/portfolio/retrieve_product_valuation.sql")

  override def process(args: String*)(implicit spark: SparkSession): Unit = {
    // TODO
  }

}
