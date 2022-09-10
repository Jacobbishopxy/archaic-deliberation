package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, Information, RegimeTask}
import regime.market.Common.{connMarket, connBizTable}

object AShareInformationWind extends RegimeTask with Information {
  lazy val query = """
  SELECT
    ad.OBJECT_ID AS object_id,
    ad.S_INFO_WINDCODE AS symbol,
    ad.S_INFO_NAME AS name,
    ad.S_INFO_COMPNAME AS company_name,
    ad.S_INFO_COMPNAMEENG  AS company_name_eng,
    ad.S_INFO_EXCHMARKET AS exchange,
    ad.S_INFO_LISTBOARD AS listboard,
    ad.S_INFO_LISTDATE AS listdate,
    ad.S_INFO_DELISTDATE AS delistdate,
    ad.S_INFO_PINYIN AS pinyin,
    ad.S_INFO_LISTBOARDNAME AS listboard_name,
    ad.IS_SHSC AS is_shsc,
    aic.WIND_IND_CODE AS wind_ind_code,
    aic.ENTRY_DT AS entry_date,
    aic.REMOVE_DT AS remove_date,
    aic.CUR_SIGN AS cur_sign,
    ac.INDUSTRIESCODE AS industry_code,
    ac.INDUSTRIESNAME AS industry_name,
    ac.LEVELNUM AS industry_level
  FROM
    ASHAREDESCRIPTION ad
  LEFT JOIN
    ASHAREINDUSTRIESCLASS aic
  ON
    ad.S_INFO_WINDCODE = aic.S_INFO_WINDCODE
  LEFT JOIN
    ASHAREINDUSTRIESCODE ac
  ON
    aic.WIND_IND_CODE = ac.INDUSTRIESCODE
  """

  lazy val countCurrentAvailable = """
  SELECT
    COUNT(*)
  FROM
    ASHAREDESCRIPTION ad
  LEFT JOIN
    ASHAREINDUSTRIESCLASS aic
  ON
    ad.S_INFO_WINDCODE = aic.S_INFO_WINDCODE
  LEFT JOIN
    ASHAREINDUSTRIESCODE ac
  ON
    aic.WIND_IND_CODE = ac.INDUSTRIESCODE
  WHERE
    ac.USED = 1 AND aic.CUR_SIGN = 1
  """

  lazy val saveTo         = "ashare_information_wind"
  lazy val primaryKeyName = "PK_ashare_information_wind"
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
        throw new Exception("Invalid command")
    }
  }
}
