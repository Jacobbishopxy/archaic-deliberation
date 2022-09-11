package regime.product

import org.apache.spark.sql.SparkSession

import regime.helper._
import regime.product.Common._

object IProductBalance extends RegimeSpark with Product {
  lazy val query = """
  SELECT
    beb.holidayMonth AS accounting_period,
    beb.tradeDate AS trade_date,
    beb.productNum AS product_num,
    beb.subjectId AS subject_id,
    beb.subjectName AS subject_name,
    beb.PreviousQtyF AS previous_qty,
    beb.PostQtyF AS post_qty,
    beb.PreviousAmt AS previous_amt,
    beb.PostAmt AS post_amt,
    beb.previousStandardAmt AS previous_std_amt,
    beb.currentStandardAmt AS current_std_amt,
    beb.DebitAmt AS debit_amt,
    beb.CreditAmt AS credit_amt,
    beb.AssetFlag AS asset_flag,
    beb.YTotalOpenQtyF AS year_total_open_qty,
    beb.YTotalCloseQtyF AS year_total_close_qty,
    bp.productName AS product_name,
    bp.NAV AS nav,
    bp.CurrentAsset AS post_asset
  FROM
    bside_ev_balancehis beb
  LEFT JOING
    bside_product bp 
  ON
    beb.productNum  = bp.productNum 
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE beb.tradeDate > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE beb.tradeDate > '$fromDate' AND beb.tradeDate < '$toDate'
  """

  lazy val readFrom       = "bside_ev_balancehis"
  lazy val saveTo         = "iproduct_balance"
  lazy val readUpdateCol  = "tradeDate"
  lazy val saveUpdateCol  = "trade_date"
  lazy val primaryKeyName = "PK_iproduct_balance"
  // TODO
  lazy val primaryColumn = Seq()
  lazy val index1        = ("IDX_iproduct_balance_1", Seq(""))
  lazy val index2        = ("IDX_iproduct_balance_2", Seq(""))

  lazy val datetimeFormat = "yyyyMMddHHmmss"
  lazy val conversionFn =
    RegimeFn.formatLongToDatetime(saveUpdateCol, saveUpdateCol, datetimeFormat)

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
