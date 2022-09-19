package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Information
import regime.market.Common._

object AIndexInformation extends Information {
  lazy val query    = RegimeSqlHelper.fromResource("sql/market/information/AIndexInformation.sql")
  lazy val readFrom = connMarketTable("AINDEXMEMBERS")
  lazy val saveTo   = connBizTable("aindex_information")
  lazy val readFromCol    = connMarketTableColumn("AINDEXMEMBERS", timeColumnMarket)
  lazy val saveToCol      = connBizTableColumn("aindex_information", timeColumnBiz)
  lazy val primaryKeyName = "PK_aindex_information"
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
        throw new Exception("Invalid Command")
    }
  }
}
