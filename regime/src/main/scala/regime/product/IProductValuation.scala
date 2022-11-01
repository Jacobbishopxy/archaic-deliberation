package regime.product

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import regime.helper._
import regime.product.Common._

object IProductValuation extends Product { 
  lazy val query = RegimeSqlHelper.fromResource("sql/product/IProductValuation.sql")
  lazy val queryFromDate = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(query, Token.timeColumnProduct, date)
  lazy val queryDateRange = (fromDate: String, toDate: String) =>
    RegimeSqlHelper.generateQueryDateRange(query, Token.timeColumnProduct, (fromDate, toDate))
  lazy val queryAtDate = (date: String) =>
    RegimeSqlHelper.generateQueryAtDate(query, Token.timeColumnProduct, date)

  lazy val readFrom       = connProductTable("ev_rpt_thirdvaluation")
  lazy val saveTo         = connBizTable("iproduct_valuation")
  lazy val readFromCol    = connProductTableColumn("ev_rpt_thirdvaluation", Token.timeColumnProduct)
  lazy val saveToCol      = connBizTableColumn("iproduct_valuation", Token.timeColumnBiz)
  lazy val primaryKeyName = "PK_ev_rpt_thirdvaluation"
  lazy val newPrimaryColName = Token.objectId
  lazy val newPKCols         = Seq("product_num", Token.timeColumnBiz)
  lazy val primaryColumn     = Seq(Token.objectId)
  lazy val index             = ("IDX_iproduct_valuation", newPKCols)

  lazy val conversionFn = RegimeFn
    .formatLongToDate(Token.timeColumnBiz, dateFormat)
    .andThen(RegimeFn.concatMultipleColumns(newPrimaryColName, newPKCols, concatenateString))
  lazy val timeReverseFn = RegimeFn.formatDateToLong(Token.timeColumnBiz, dateFormat)

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
