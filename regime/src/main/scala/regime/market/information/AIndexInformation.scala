package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, Information, RegimeTask}
import regime.market.Common.{connMarket, connBizTable}

object AIndexInformation extends RegimeTask with Information {
  lazy val query = """
  SELECT
    ad.OBJECT_ID AS object_id,
    am.S_INFO_WINDCODE,
    am.S_CON_WINDCODE,
    am.S_CON_INDATE,
    am.S_CON_OUTDATE,
    am.CUR_SIGN,
    am.OPDATE AS update_date,
    ad.S_INFO_NAME AS index_abbr,
    ad.S_INFO_COMPNAME AS index_name,
    ad.S_INFO_EXCHMARKET AS exchange,
    ad.S_INFO_INDEX_BASEPER AS index_base_per,
    ad.S_INFO_INDEX_BASEPT AS index_base_pt,
    ad.S_INFO_LISTDATE AS list_date,
    ad.S_INFO_INDEX_WEIGHTSRULE AS index_weights_rule,
    ad.S_INFO_PUBLISHER AS publisher,
    ad.S_INFO_INDEXCODE AS index_code,
    ad.S_INFO_INDEXSTYLE AS index_style,
    ad.INDEX_INTRO AS index_intro,
    ad.WEIGHT_TYPE AS weight_type,
    ad.EXPIRE_DATE AS expire_date,
    ad.OPDATE AS description_update_date
  FROM
    AINDEXMEMBERS am
  LEFT JOIN
    AINDEXDESCRIPTION ad
  ON
    am.S_INFO_WINDCODE = ad.S_INFO_WINDCODE
  """

  lazy val saveTo         = "aindex_information"
  lazy val primaryKeyName = "PK_aindex_information"
  lazy val primaryColumn  = Seq("object_id")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(connMarket, query, connBizTable(saveTo))
        createPrimaryKey(connBizTable(saveTo), primaryKeyName, primaryColumn)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKey(connBizTable(saveTo), primaryKeyName, primaryColumn)
      case Command.SyncAll :: _ =>
        syncReplaceAll(connMarket, query, connBizTable(saveTo))
      case _ =>
        throw new Exception("Invalid Command")
    }
  }
}
