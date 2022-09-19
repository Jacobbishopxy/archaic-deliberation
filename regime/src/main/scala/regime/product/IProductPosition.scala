package regime.product

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import regime.helper._
import regime.product.Common._

object IProductPosition extends Product {
  lazy val query = RegimeSqlHelper.fromResource("sql/product/IProductPosition.sql")
  lazy val queryFromDate = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(query, timeColumnProduct, date)
  lazy val queryDateRange = (fromDate: String, toDate: String) =>
    RegimeSqlHelper.generateQueryDateRange(query, timeColumnProduct, (fromDate, toDate))
  lazy val queryAtDate = (date: String) =>
    RegimeSqlHelper.generateQueryAtDate(query, timeColumnProduct, date)

  lazy val readFrom       = connProductTable("bside_ev_rpt_tradesummary")
  lazy val saveTo         = connBizTable("iproduct_position")
  lazy val readFromCol    = connProductTableColumn("bside_ev_rpt_tradesummary", timeColumnProduct)
  lazy val saveToCol      = connBizTableColumn("iproduct_position", timeColumnBiz)
  lazy val primaryKeyName = "PK_iproduct_position"
  lazy val newPrimaryColName = "object_id"
  lazy val newPKCols = Seq(
    "trade_date",
    "product_num",
    "product_account_type",
    "exch_id",
    "stock_id",
    "bs_flag",
    "hedge_flag"
  )
  lazy val primaryColumn = Seq("object_id")
  lazy val index1 = (
    "IDX_iproduct_position_1",
    Seq(timeColumnBiz, "product_num", "parent_product_num", "exch_id", "stock_id")
  )
  lazy val index2 = (
    "IDX_iproduct_position_2",
    Seq(timeColumnBiz, "exch_id", "stock_id")
  )

  lazy val conversionFn = RegimeFn
    .formatLongToDatetime(timeColumnBiz, datetimeFormat)
    .andThen(RegimeFn.whenNotInThen("suspended_flag", Seq("F", "N", "T", "Y", "0"), "0"))
    .andThen(RegimeFn.concatMultipleColumns(newPrimaryColName, newPKCols, concatenateString))
  lazy val timeReverseFn = RegimeFn.formatDatetimeToLong("max_trade_date", datetimeFormat)

  def process(args: String*)(implicit spark: SparkSession): Unit = args.toList match {
    case Command.Initialize :: _ =>
      val bo = RegimeSyncHelper
        .generateBatchOption(readFromCol, true, fetchSize)
        .getOrElse(throw new Exception("generateBatchOption failed"))
      syncInitAll(readFrom, saveTo, query, Some(bo), conversionFn)
      createPrimaryKeyAndIndex(
        saveTo,
        (primaryKeyName, primaryColumn),
        Seq(index1, index2)
      )
    case Command.ExecuteOnce :: _ =>
      cleanNullData(saveTo, newPKCols, "or")
      createPrimaryKeyAndIndex(
        saveTo,
        (primaryKeyName, primaryColumn),
        Seq(index1, index2)
      )
    case Command.SyncFromLastUpdate :: _ =>
      syncInsertFromLastUpdate(
        readFromCol,
        saveToCol,
        queryFromDate,
        None,
        Some(timeReverseFn),
        conversionFn
      )
    case Command.OverrideFromLastUpdate :: _ =>
      syncUpsertFromLastUpdate(
        readFromCol,
        saveToCol,
        primaryColumn,
        queryFromDate,
        None,
        Some(timeReverseFn),
        conversionFn
      )
    case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
      val tf = convertStringToLongLikeDatetimeString(timeFrom)
      syncUpsert(
        readFrom,
        saveTo,
        queryFromDate(tf),
        primaryColumn,
        None,
        conversionFn
      )
    case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
      val tf = convertStringToLongLikeDatetimeString(timeFrom)
      val tt = convertStringToLongLikeDatetimeString(timeTo)
      syncUpsert(
        readFrom,
        saveTo,
        queryDateRange(tf, tt),
        primaryColumn,
        None,
        conversionFn
      )
    case c @ _ =>
      log.error(c)
      throw new Exception("Invalid command")
  }
}
