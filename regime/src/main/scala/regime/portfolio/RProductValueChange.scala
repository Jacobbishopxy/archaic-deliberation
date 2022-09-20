package regime.portfolio

import org.apache.spark.sql.SparkSession

import regime.helper._
import regime.portfolio.helper._

object RProductValueChange extends Portfolio {
  lazy val query = RegimeSqlHelper.fromResource("sql/portfolio/calculate_history_product_value.sql")

  override def process(args: String*)(implicit spark: SparkSession): Unit = {
    // TODO
  }

}
