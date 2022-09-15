package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Information
import regime.market.Common.{connMarketTable, connBizTable}

object AShareCalendar extends RegimeSpark with Information {
  lazy val query      = RegimeSqlHelper.fromResource("sql/market/information/AShareCalendar.sql")
  lazy val readFrom   = connMarketTable("ASHARECALENDAR")
  lazy val saveTo     = connBizTable("ashare_calendar")
  lazy val primaryKey = ("PK_ashare_calendar", Seq("object_id"))
  lazy val index      = ("IDX_ashare_calendar", Seq("trade_days"))

  def process(args: String*)(implicit spark: SparkSession): Unit =
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None)
        createPrimaryKeyAndIndex(
          saveTo,
          primaryKey,
          Seq(index)
        )
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          saveTo,
          primaryKey,
          Seq(index)
        )
      case Command.SyncAll :: _ =>
        syncReplaceAll(readFrom, saveTo, query, None)
      case _ =>
        throw new Exception("Invalid command")
    }

}
