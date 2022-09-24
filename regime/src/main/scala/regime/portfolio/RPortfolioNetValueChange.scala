package regime.portfolio

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import regime.helper._
import regime.portfolio.helper._
import regime.portfolio.Common._
import org.apache.spark.sql.SaveMode

object RPortfolioNetValueChange extends Portfolio {

  lazy val calcNetValueChgD =
    RegimeSqlHelper.fromResource("sql/portfolio/calculate_net_value_chg_d.sql")
  lazy val calcNetValueChgM =
    RegimeSqlHelper.fromResource("sql/portfolio/calculate_net_value_chg_m.sql")
  lazy val calcNetValueChgQ =
    RegimeSqlHelper.fromResource("sql/portfolio/calculate_net_value_chg_q.sql")
  lazy val calcNetValueChgY =
    RegimeSqlHelper.fromResource("sql/portfolio/calculate_net_value_chg_y.sql")

  lazy val calcFromDateD = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(calcNetValueChgD, Token.tradeDate, date)
  lazy val calcFromDateM = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(calcNetValueChgM, Token.tradeDate, date)
  lazy val calcFromDateQ = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(calcNetValueChgQ, Token.tradeDate, date)
  lazy val calcFromDateY = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(calcNetValueChgY, Token.tradeDate, date)

  lazy val rawDataTableName  = "iproduct_valuation"
  lazy val tableName         = "rportfolio_net_value_change"
  lazy val sourceTableColumn = (rawDataTableName, Token.tradeDate)
  lazy val targetTableColumn = (tableName, Token.tradeDate)
  lazy val readFrom          = connTable(rawDataTableName)
  lazy val saveTo            = connTable(tableName)
  lazy val saveToCol         = connTableColumn(tableName, Token.tradeDate)

  lazy val primaryKeyName = s"PK_$tableName"
  lazy val primaryKeyCols = Seq(Token.productNum, Token.tradeDate)

  def calcAll(
      helper: RegimeJdbcHelper,
      date: Option[String]
  )(implicit spark: SparkSession): DataFrame = {
    // read from different calc methods
    val (nvcD, nvcM, nvcQ, nvcY) = date match {
      case None =>
        (
          helper.readTable(calcNetValueChgD),
          helper.readTable(calcNetValueChgM),
          helper.readTable(calcNetValueChgQ),
          helper.readTable(calcNetValueChgY)
        )
      case Some(d) =>
        (
          helper.readTable(calcFromDateD(d)),
          helper.readTable(calcFromDateM(d)),
          helper.readTable(calcFromDateQ(d)),
          helper.readTable(calcFromDateY(d))
        )
    }

    nvcD
      .join(
        nvcM,
        nvcD(Token.productNum) === nvcM(Token.productNum) &&
          nvcD(Token.tradeDate) === nvcM(Token.tradeDate),
        "left"
      )
      .drop(nvcM(Token.productNum))
      .drop(nvcM(Token.tradeDate))
      .drop(nvcM(Token.netValue))
      .join(
        nvcQ,
        nvcD(Token.productNum) === nvcQ(Token.productNum) &&
          nvcD(Token.tradeDate) === nvcQ(Token.tradeDate),
        "left"
      )
      .drop(nvcQ(Token.productNum))
      .drop(nvcQ(Token.tradeDate))
      .drop(nvcQ(Token.netValue))
      .join(
        nvcY,
        nvcD(Token.productNum) === nvcY(Token.productNum) &&
          nvcD(Token.tradeDate) === nvcY(Token.tradeDate),
        "left"
      )
      .drop(nvcY(Token.productNum))
      .drop(nvcY(Token.tradeDate))
      .drop(nvcY(Token.netValue))
  }

  override def process(args: String*)(implicit spark: SparkSession): Unit =
    args.toList match {
      case Command.Initialize :: _ =>
        val helper = RegimeJdbcHelper(conn)
        val df     = calcAll(helper, None)
        helper.saveTable(df, tableName, SaveMode.ErrorIfExists)
        createPrimaryKey(saveTo, primaryKeyName, primaryKeyCols)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKey(saveTo, primaryKeyName, primaryKeyCols)
      case Command.SyncFromLastUpdate :: _ =>
        RegimeCalcHelper.insertFromLastUpdateTime(
          conn,
          sourceTableColumn,
          targetTableColumn,
          None,
          (helper: RegimeJdbcHelper, date: String) => calcAll(helper, Some(date))
        )
      case Command.OverrideFromLastUpdate :: _ =>
        RegimeCalcHelper.upsertFromLastUpdateTime(
          conn,
          sourceTableColumn,
          targetTableColumn,
          primaryKeyCols,
          None,
          (helper: RegimeJdbcHelper, date: String) => calcAll(helper, Some(date))
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        val helper = RegimeJdbcHelper(conn)
        val df     = calcAll(helper, Some(timeFrom))
        helper.upsertTable(
          df,
          tableName,
          None,
          false,
          primaryKeyCols,
          UpsertAction.DoUpdate
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
}
