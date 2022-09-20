package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Information
import regime.market.Common._

object AShareCalendar extends Information {
  lazy val query       = RegimeSqlHelper.fromResource("sql/market/information/AShareCalendar.sql")
  lazy val readFrom    = connMarketTable("ASHARECALENDAR")
  lazy val saveTo      = connBizTable("ashare_calendar")
  lazy val readFromCol = connMarketTableColumn("ASHARECALENDAR", Token.timeColumnMarket)
  lazy val saveToCol   = connBizTableColumn("ashare_calendar", Token.timeColumnBiz)
  lazy val primaryKey  = ("PK_ashare_calendar", Seq(Token.objectId))
  lazy val index1      = ("IDX_ashare_calendar_1", Seq(Token.timeTradeDate))
  lazy val index2      = ("IDX_ashare_calendar_2", Seq(Token.timeColumnBiz))

  lazy val conversionFn = RegimeFn.formatStringToDate(Token.timeTradeDate, dateFormat)

  def process(args: String*)(implicit spark: SparkSession): Unit =
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None, conversionFn)
        createPrimaryKeyAndIndex(
          saveTo,
          primaryKey,
          Seq(index1, index2)
        )
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          saveTo,
          primaryKey,
          Seq(index1, index2)
        )
      case Command.SyncAll :: _ =>
        syncReplaceAllIfUpdated(readFromCol, saveToCol, query, None, conversionFn)
      case _ =>
        throw new Exception("Invalid command")
    }
}
