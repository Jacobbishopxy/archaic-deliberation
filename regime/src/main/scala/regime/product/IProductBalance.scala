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

  lazy val readFrom          = "bside_ev_balancehis"
  lazy val saveTo            = "iproduct_balance"
  lazy val readUpdateCol     = "tradeDate"
  lazy val saveUpdateCol     = "trade_date"
  lazy val primaryKeyName    = "PK_iproduct_balance"
  lazy val newPrimaryColName = "object_id"
  lazy val newPKCols         = Seq("trade_date", "product_num", "subject_id")
  lazy val primaryColumn     = Seq("object_id")
  lazy val index1            = ("IDX_iproduct_balance_1", Seq("product_num"))
  lazy val index2            = ("IDX_iproduct_balance_2", Seq("subject_id"))

  lazy val conversionFn = RegimeFn
    .formatLongToDatetime(saveUpdateCol, datetimeFormat)
    .andThen(RegimeFn.concatMultipleColumns(newPrimaryColName, newPKCols, concatenateString))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(
          connProduct,
          query,
          connBizTable(saveTo),
          conversionFn
        )
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
          (primaryKeyName, primaryColumn),
          Seq(index1, index2)
        )
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
          (primaryKeyName, primaryColumn),
          Seq(index1, index2)
        )
      case Command.SyncFromLastUpdate :: _ =>
        syncInsertFromLastUpdate(
          connProductTableColumn(readFrom, readUpdateCol),
          connBizTableColumn(saveTo, saveUpdateCol),
          queryFromDate,
          conversionFn
        )
      case Command.OverrideFromLastUpdate :: _ =>
        syncUpsertFromLastUpdate(
          connProductTableColumn(readFrom, readUpdateCol),
          connBizTableColumn(saveTo, saveUpdateCol),
          primaryColumn,
          queryFromDate,
          conversionFn
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          connProduct,
          queryFromDate(timeFrom),
          connBizTable(saveTo),
          primaryColumn,
          conversionFn
        )
      case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
        syncUpsert(
          connProduct,
          queryDateRange(timeFrom, timeTo),
          connBizTable(saveTo),
          primaryColumn,
          conversionFn
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
  }
}
