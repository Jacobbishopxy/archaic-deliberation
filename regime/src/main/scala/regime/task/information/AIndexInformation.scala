package regime.task.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.task.{Command, Information, RegimeTask}
import regime.task.Common.{connMarket, connBiz}

object AIndexInformation extends RegimeTask with Information {
  val appName: String = "AIndexInformation"

  val query = """
  SELECT
    am.OBJECT_ID AS object_id,
    am.S_INFO_WINDCODE,
    am.S_CON_WINDCODE,
    am.S_CON_INDATE,
    am.S_CON_OUTDATE,
    am.CUR_SIGN,
    ad.S_INFO_WINDCODE
    ad.S_INFO_CODE
    ad.S_INFO_NAME
    ad.S_INFO_COMPNAME
    ad.S_INFO_EXCHMARKET
    ad.S_INFO_INDEX_BASEPER
    ad.S_INFO_INDEX_BASEPT
    ad.S_INFO_LISTDATE
    ad.S_INFO_INDEX_WEIGHTSRULE
    ad.S_INFO_PUBLISHER
    ad.S_INFO_INDEXCODE
    ad.S_INFO_INDEXSTYLE
    ad.INDEX_INTRO
    ad.WEIGHT_TYPE
    ad.EXPIRE_DATE
  FROM
    AINDEXMEMBERS am
  LEFT JOIN
    AINDEXDESCRIPTION ad
  ON
    am.S_INFO_WINDCODE = ad.S_INFO_WINDCODE
  """

  val saveTo         = "aindex_information"
  val primaryKeyName = "PK_aindex_information"
  val primaryColumn  = Seq("object_id")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.SyncAll :: _ =>
        syncAll(connMarket, query, connBiz, saveTo)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKey(connMarket, saveTo, primaryKeyName, primaryColumn)
      case _ =>
        throw new Exception("Invalid Command")
    }
  }
}
