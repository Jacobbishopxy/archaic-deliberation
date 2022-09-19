package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Information
import regime.market.Common._

object AShareInformationCitics extends Information {
  lazy val query =
    RegimeSqlHelper.fromResource("sql/market/information/AShareInformationCitics.sql")
  lazy val readFrom       = connMarketTable("ASHAREDESCRIPTION")
  lazy val saveTo         = connBizTable("ashare_information_citics")
  lazy val readFromCol    = connMarketTableColumn("ASHAREDESCRIPTION", timeColumnMarket)
  lazy val saveToCol      = connBizTableColumn("ashare_information_citics", timeColumnBiz)
  lazy val primaryKeyName = "PK_ashare_information_citics"
  lazy val primaryColumn  = Seq("object_id")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None)
        createPrimaryKey(saveTo, primaryKeyName, primaryColumn)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKey(saveTo, primaryKeyName, primaryColumn)
      case Command.SyncAll :: _ =>
        syncReplaceAllIfUpdated(readFromCol, saveToCol, query, None)
      case _ =>
        throw new Exception("Invalid command")
    }
  }
}
