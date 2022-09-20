package regime.portfolio.helper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import regime.Conn
import regime.helper._
import regime.portfolio.Common._

class ProductHelper(helper: RegimeJdbcHelper)(implicit spark: SparkSession) {
  import spark.implicits._

  lazy val tv = "PRODUCT"
  lazy val products = helper.readTable(
    RegimeSqlHelper.fromResource("sql/portfolio/retrieve_calendar.sql")
  )

  def findActiveProducts(): DataFrame =
    products.select("product_num", "product_id", "product_name")

}
