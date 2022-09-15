package regime.product

import org.apache.spark.sql.SparkSession

import regime.helper._
import regime.product.Common._

object IProductInformation extends RegimeSpark with Product {
  lazy val query          = RegimeSqlHelper.fromResource("sql/product/IProductInformation.sql")
  lazy val readFrom       = connProductTable("bside_product")
  lazy val saveTo         = connBizTable("iproduct_information")
  lazy val primaryKeyName = "PK_iproduct_information"
  lazy val primaryColumn  = Seq("product_num")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None)
        createPrimaryKey(saveTo, primaryKeyName, primaryColumn)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKey(saveTo, primaryKeyName, primaryColumn)
      case Command.SyncAll :: _ =>
        syncReplaceAll(readFrom, saveTo, query, None)
      case _ =>
        throw new Exception("Invalid Command")
    }
  }
}
