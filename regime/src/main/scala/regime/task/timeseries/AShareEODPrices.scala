package regime.task.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.SparkTaskCommon
import regime.task.Common.{connMarket, connBiz}

// TODO:
// 1. append by date
// 1. replace by date
// 1. check trade_calender
object AShareEODPrices extends SparkTaskCommon {
  val appName: String = "AShareEODPrices ETL"

  val querySyncAll = """
  SELECT
    S_INFO_WINDCODE AS symbol,
    TRADE_DT AS trade_date,
    CRNCY_CODE AS currency,
    S_DQ_PRECLOSE AS pre_close,
    S_DQ_OPEN AS open,
    S_DQ_HIGH AS high,
    S_DQ_LOW AS low,
    S_DQ_CLOSE AS close,
    S_DQ_CHANGE AS change,
    S_DQ_PCTCHANGE AS percent_change,
    S_DQ_VOLUME AS volume,
    S_DQ_AMOUNT AS amount,
    S_DQ_ADJPRECLOSE AS adj_pre_close,
    S_DQ_ADJOPEN AS adj_open,
    S_DQ_ADJHIGH AS adj_high,
    S_DQ_ADJLOW AS adj_low,
    S_DQ_ADJCLOSE AS adj_close,
    S_DQ_ADJFACTOR AS adj_factor,
    S_DQ_AVGPRICE AS average_price,
    S_DQ_TRADESTATUS AS trade_status,
    S_DQ_TRADESTATUSCODE AS trade_status_code,
    S_DQ_LIMIT AS limit_price,
    S_DQ_STOPPING AS stopping_price
  FROM
    ASHAREEODPRICES
  """

  val queryDaily = s"""
  TODO
  """

  val save_to = "ashare_eod_prices"

  def process(spark: SparkSession, args: String*): Unit = {

    // TODO
  }

}
