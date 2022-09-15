package regime.product

import org.apache.spark.sql.SparkSession

import regime.helper._
import regime.product.Common._

object IProductBalance extends RegimeSpark with Product {
  lazy val query = RegimeSqlHelper.fromResource("sql/product/IProductBalance.sql")
  lazy val queryFromDate = (date: String) => query + s"""
  WHERE beb.tradeDate > '$date'
  """
  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE beb.tradeDate > '$fromDate' AND beb.tradeDate < '$toDate'
  """

  lazy val readFrom          = connProductTable("bside_ev_balancehis")
  lazy val saveTo            = connBizTable("iproduct_balance")
  lazy val readFromCol       = connProductTableColumn("bside_ev_balancehis", "tradeDate")
  lazy val saveToCol         = connBizTableColumn("iproduct_balance", "trade_date")
  lazy val primaryKeyName    = "PK_iproduct_balance"
  lazy val newPrimaryColName = "object_id"
  lazy val newPKCols         = Seq("trade_date", "product_num", "subject_id")
  lazy val primaryColumn     = Seq("object_id")
  lazy val index1            = ("IDX_iproduct_balance_1", Seq("product_num"))
  lazy val index2            = ("IDX_iproduct_balance_2", Seq("subject_id"))

  lazy val conversionFn = RegimeFn
    .formatLongToDatetime("trade_date", datetimeFormat)
    .andThen(RegimeFn.concatMultipleColumns(newPrimaryColName, newPKCols, concatenateString))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None)
        createPrimaryKeyAndIndex(
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq(index1, index2)
        )
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq(index1, index2)
        )
      case Command.SyncFromLastUpdate :: _ =>
      // TODO
      case Command.OverrideFromLastUpdate :: _ =>
      // TODO
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          readFrom,
          saveTo,
          queryFromDate(timeFrom),
          primaryColumn,
          None
        )
      case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
        syncUpsert(
          readFrom,
          saveTo,
          queryDateRange(timeFrom, timeTo),
          primaryColumn,
          None
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
  }
}
