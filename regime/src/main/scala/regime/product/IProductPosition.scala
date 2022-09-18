package regime.product

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import regime.helper._
import regime.product.Common._

object IProductPosition extends Product {
  lazy val query = RegimeSqlHelper.fromResource("sql/product/IProductPosition.sql")
  lazy val queryFromDate = (date: String) => query + s"""
  WHERE tradeDate > '$date'
  """
  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE tradeDate > '$fromDate' AND tradeDate < '$toDate'
  """
  lazy val queryAtDate = (date: String) => query + s"""
  WHERE tradeDate = '$date'
  """

  lazy val readFrom          = connProductTable("bside_ev_rpt_tradesummary")
  lazy val saveTo            = connBizTable("iproduct_position")
  lazy val readFromCol       = connProductTableColumn("bside_ev_rpt_tradesummary", "tradeDate")
  lazy val saveToCol         = connBizTableColumn("iproduct_position", "trade_date")
  lazy val primaryKeyName    = "PK_iproduct_position"
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
    Seq("trade_date", "product_num", "parent_product_num", "exch_id", "stock_id")
  )
  lazy val index2 = (
    "IDX_iproduct_position_2",
    Seq("trade_date", "exch_id", "stock_id")
  )

  lazy val conversionFn = RegimeFn
    .formatLongToDatetime("trade_date", datetimeFormat)
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
