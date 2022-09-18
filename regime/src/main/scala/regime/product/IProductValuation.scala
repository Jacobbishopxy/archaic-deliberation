package regime.product

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import regime.helper._
import regime.product.Common._

object IProductValuation extends Product {
  lazy val query = RegimeSqlHelper.fromResource("sql/product/IProductValuation.sql")
  lazy val queryFromDate = (date: String) => query + s"""
  WHERE tradeDate > '$date'
  """
  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE tradeDate > '$fromDate' AND tradeDate < '$toDate'
  """
  lazy val queryAtDate = (date: String) => query + s"""
  WHERE tradeDate = '$date'
  """

  lazy val readFrom          = connProductTable("ev_rpt_thirdvaluation")
  lazy val saveTo            = connBizTable("iproduct_valuation")
  lazy val readFromCol       = connProductTableColumn("ev_rpt_thirdvaluation", "tradeDate")
  lazy val saveToCol         = connBizTableColumn("iproduct_valuation", "trade_date")
  lazy val primaryKeyName    = "PK_ev_rpt_thirdvaluation"
  lazy val newPrimaryColName = "object_id"
  lazy val newPKCols         = Seq("product_num", "trade_date")
  lazy val primaryColumn     = Seq("object_id")
  lazy val index             = ("IDX_iproduct_valuation", newPKCols)

  lazy val conversionFn = RegimeFn
    .formatLongToDatetime("trade_date", datetimeFormat)
    .andThen(RegimeFn.concatMultipleColumns(newPrimaryColName, newPKCols, concatenateString))
  lazy val reverseCvtFn = RegimeFn.formatDatetimeToLong("max_trade_date", datetimeFormat)

  def process(args: String*)(implicit spark: SparkSession): Unit = args.toList match {
    case Command.Initialize :: _ =>
      syncInitAll(readFrom, saveTo, query, None, conversionFn)
      createPrimaryKeyAndIndex(
        saveTo,
        (primaryKeyName, primaryColumn),
        Seq(index)
      )
    case Command.ExecuteOnce :: _ =>
      cleanNullData(saveTo, newPKCols, "or")
      createPrimaryKeyAndIndex(
        saveTo,
        (primaryKeyName, primaryColumn),
        Seq(index)
      )
    case Command.SyncFromLastUpdate :: _ =>
      syncInsertFromLastUpdate(
        readFromCol,
        saveToCol,
        queryFromDate,
        None,
        Some(reverseCvtFn),
        conversionFn
      )
    case Command.OverrideFromLastUpdate :: _ =>
      syncUpsertFromLastUpdate(
        readFromCol,
        saveToCol,
        primaryColumn,
        queryFromDate,
        None,
        Some(reverseCvtFn),
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
