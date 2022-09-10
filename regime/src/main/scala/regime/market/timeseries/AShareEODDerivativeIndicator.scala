package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, TimeSeries, RegimeTask}
import regime.market.Common.{connMarket, connBiz, connBizTable}

object AShareEODDerivativeIndicator extends RegimeTask with TimeSeries {
  val appName: String = "AShareEODDerivativeIndicator"

  val query = """
  SELECT
    OBJECT_ID AS object_id,
    S_INFO_WINDCODE AS symbol,
    TRADE_DT AS trade_date,
    CRNCY_CODE AS currency,
    S_VAL_MV AS val_mv,
    S_DQ_MV AS dq_mv,
    S_PQ_HIGH_52W_ AS pq_high_52w,
    S_PQ_LOW_52W_ AS pq_low_52w,
    S_VAL_PE AS val_pe,
    S_VAL_PB_NEW AS val_pb_new,
    S_VAL_PE_TTM AS val_pe_ttm,
    S_VAL_PCF_OCF AS val_pcf_ocf,
    S_VAL_PCF_OCFTTM AS val_pcf_ocf_ttm,
    S_VAL_PCF_NCF AS val_pcf_ncf,
    S_VAL_PCF_NCFTTM AS val_pcf_ncf_ttm,
    S_VAL_PS AS val_ps,
    S_VAL_PS_TTM AS val_ps_ttm,
    S_DQ_TURN AS dq_turn,
    S_DQ_FREETURNOVER AS dq_free_turnover,
    TOT_SHR_TODAY AS tot_shr_today,
    FLOAT_A_SHR_TODAY AS float_a_shr_today,
    S_DQ_CLOSE_TODAY AS dq_close_today,
    S_PRICE_DIV_DPS AS price_div_dps,
    S_PQ_ADJHIGH_52W AS pq_adj_high_52w,
    S_PQ_ADJLOW_52W AS pq_adj_low_52w,
    FREE_SHARES_TODAY AS free_shares_today,
    NET_PROFIT_PARENT_COMP_TTM AS net_profit_parent_comp_ttm,
    NET_PROFIT_PARENT_COMP_LYR AS net_profit_parent_comp_lyr,
    NET_ASSETS_TODAY AS net_assets_today,
    NET_CASH_FLOWS_OPER_ACT_TTM AS cashflow_oper_act_ttm,
    NET_CASH_FLOWS_OPER_ACT_LYR AS cashflow_oper_act_lyr,
    OPER_REV_TTM AS oper_rev_ttm,
    OPER_REV_LYR AS oper_rev_lyr,
    NET_INCR_CASH_CASH_EQU_TTM AS net_incr_cash_cash_equ_ttm,
    NET_INCR_CASH_CASH_EQU_LYR AS net_incr_cash_cash_equ_lyr,
    UP_DOWN_LIMIT_STATUS AS up_down_limit_status,
    LOWEST_HIGHEST_STATUS AS lowest_highest_status,
    OPDATE AS update_date
  FROM
    ASHAREEODDERIVATIVEINDICATOR
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  val saveTo         = "ashare_eod_derivative_indicator"
  val primaryKeyName = "PK_ashare_eod_derivative_indicator"
  val primaryColumn  = Seq("object_id")
  val index1         = ("IDX_ashare_eod_derivative_indicator_1", Seq("update_date"))
  val index2         = ("IDX_ashare_eod_derivative_indicator_2", Seq("trade_date", "symbol"))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.SyncAll :: _ =>
        syncAll(connMarket, query, connBiz, saveTo)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
          (primaryKeyName, primaryColumn),
          Seq(index1, index2)
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          connMarket,
          queryFromDate(timeFrom),
          connBiz,
          primaryColumn,
          saveTo
        )
      case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
        syncUpsert(
          connMarket,
          queryDateRange(timeFrom, timeTo),
          connBiz,
          primaryColumn,
          saveTo
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
  }

}
