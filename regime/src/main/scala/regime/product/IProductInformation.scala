package regime.product

import org.apache.spark.sql.SparkSession

import regime.helper._
import regime.product.Common.{connProduct, connBizTable}

object IProductInformation extends RegimeSpark with Product {
  lazy val query          = RegimeSqlHelper.fromResource("sql/product/IProductInformation.sql")
  lazy val saveTo         = "iproduct_information"
  lazy val primaryKeyName = "PK_iproduct_information"
  lazy val primaryColumn  = Seq("product_num")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(connProduct, query, connBizTable(saveTo))
        createPrimaryKey(connBizTable(saveTo), primaryKeyName, primaryColumn)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKey(connBizTable(saveTo), primaryKeyName, primaryColumn)
      case Command.SyncAll :: _ =>
        syncReplaceAll(connProduct, query, connBizTable(saveTo))
      case _ =>
        throw new Exception("Invalid Command")
    }
  }
}
