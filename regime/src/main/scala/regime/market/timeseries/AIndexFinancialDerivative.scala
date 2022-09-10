package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, TimeSeries, RegimeTask}
import regime.market.Common._

object AIndexFinancialDerivative extends RegimeTask with TimeSeries {
  lazy val query = """
  SELECT
    OBJECT_ID AS object_id,
    S_INFO_WINDCODE AS symbol,
    REPORT_PERIOD AS report_period,
    INGREDIENT_NUM,
    LOSS_INGREDIENT_NUM,
    NET_PROFIT_TOT,
    OPER_REV,
    S_VAL_PCF_OCF,
    NET_INCR_CASH_CASH_EQU_TOT,
    TOTAL_ASSETS,
    TOTAL_NET_ASSETS,
    TOTAL_INVENTORY,
    TOTAL_ACCOUNTS_RECEIVABLE,
    ROE,
    ROA,
    NET_PROFIT_RATE,
    GROSS_PROFIT_MARGIN,
    ASSETS_LIABILITIES,
    PERIOD_EXPENSE_RATE,
    FINANCIAL_LEVERAGE,
    ASSET_TURNOVER,
    NET_ASSET_TURNOVER,
    S_FA_INVTURNDAYS,
    S_FA_ARTURNDAYS,
    NET_BUSINESS_CYCLE,
    NET_CASHFLOW_PROFIT,
    CASHFLOW_INCOME_RATIO,
    NET_PROFIT_TTM,
    OPER_REV_TTM,
    S_VAL_PCF_OCF_TTM,
    NET_INCR_CASH_CASH_EQU_TTM,
    ROE_TTM,
    ROA_TTM,
    NET_PROFIT_RATE_TTM,
    NET_PROFIT_GROWTH_RATE,
    NET_PROFIT_GROWTH_RATE_TTM,
    NET_PROFIT_GROWTH_RATE_QUA,
    NET_PROFIT_GROWTH_RATE_CHAIN,
    OPER_REV_GROWTH_RATE,
    OPER_REV_GROWTH_RATE_TTM,
    OPER_REV_GROWTH_RATE_QUA,
    OPER_REV_GROWTH_RATE_CHAIN,
    S_VAL_PCF_OCF_GROWTH_RATE,
    S_VAL_PCF_OCF_GROWTH_RATE_TTM,
    S_VAL_PCF_OCF_QUA,
    S_VAL_PCF_OCF_CHAIN,
    ROE_INCREASE_LESS,
    ROE_INCREASE_LESS_TTM,
    ROE_INCREASE_LESS_QUA,
    ROE_INCREASE_LESS_CHAIN,
    ROA_INCREASE_LESS,
    ROA_INCREASE_LESS_TTM,
    ROA_INCREASE_LESS_QUA,
    ROA_INCREASE_LESS_CHAIN,
    NET_PRO_RATE_INCREASE_LESS,
    NET_PRO_RATE_INC_LESS_TTM,
    NET_PRO_RATE_INC_LESS_QUA,
    NET_PRO_RATE_INC_LESS_CHAIN,
    GROSSPROFIT_MARGIN_INC_LESS,
    GROSS_MARGIN_INC_LESS_QUA,
    GROSS_MARGIN_INC_LESS_CHAIN,
    PERIOD_EXPENSE_INC_LESS,
    PERIOD_EXPENSE_INC_LESS_QUA,
    PERIOD_EXPENSE_INC_LESS_CHAIN,
    NET_ASSETS_GROWTH_RATE,
    ASSETS_GROWTH_RATE,
    STOCK_RATIO_GROWTH_RATE,
    ACCOUNTS_GROWTH_RATE,
    REPORT_TYPE_CODE,
    S_FA_CURRENT,
    S_FA_QUICK,
    SALES_EXPENSE_RATE,
    SALES_EXPENSE_RATE_QUA,
    PERIOD_EXPENSE_RATE_QUA,
    NET_PROFIT_RATE_QUA,
    PROFIT_RATE_QUA,
    ROE_QUA,
    ROA_QUA,
    OPER_COST_TOT,
    SELLING_DIST_EXP_TOT,
    GERL_ADMIN_EXP_TOT,
    FIN_EXP_TOT,
    IMPAIR_LOSS_ASSETS_TOT,
    NET_GAIN_CHG_FV_TOT,
    NET_INVEST_INC_TOT,
    OPER_PROFIT_TOT,
    NON_OPER_REV_TOT,
    NON_OPER_EXP_TOT,
    TOT_PROFIT,
    INC_TAX_TOT,
    NET_PROFIT_INCL_MIN_INT_INC,
    NET_AFTER_DED_NR_LP_CORRECT,
    S_FA_EXTRAORDINARY,
    S_FA_SALESCASHINTOOR,
    STOT_CASH_INFLOWS_OPER_TOT,
    CASH_PAY_GOODS_PURCH_SERV_REC,
    CASH_PAY_BEH_EMPL,
    STOT_CASH_OUTFLOWS_OPER_TOT,
    NET_CASH_RECP_DISP_FIOLTA,
    STOT_CASH_INFLOWS_INV_TOT,
    CASH_PAY_ACQ_CONST_FIOLTA,
    CASH_PAID_INVEST,
    CASH_PAID_INVEST_TOT,
    NET_CASH_FLOWS_INV_TOT,
    CASH_RECP_CAP_CONTRIB,
    CASH_RECP_BORROW,
    PROC_ISSUE_BONDS,
    STOT_CASH_INFLOWS_FNC_TOT,
    CASH_PREPAY_AMT_BORR,
    CASH_PAY_DIST_DPCP_INT_EXP,
    STOT_CASH_OUTFLOWS_FNC_TOT,
    NET_CASH_FLOWS_FNC_TOT,
    CASH_CASH_EQU_END_PERIOD,
    MONETARY_CAP,
    NOTES_RCV,
    ACCT_RCV,
    PREPAY,
    TOT_CUR_ASSETS,
    LONG_TERM_EQY_INVEST,
    INVEST_REAL_ESTATE,
    FIX_ASSETS,
    CONST_IN_PROG,
    TOT_NON_CUR_ASSETS,
    ST_BORROW,
    NOTES_PAYABLE,
    ACCT_PAYABLE,
    ADV_FROM_CUST,
    EMPL_BEN_PAYABLE,
    NON_CUR_LIAB_DUE_WITHIN_1Y,
    TOT_CUR_LIAB,
    LT_BORROW,
    BONDS_PAYABLE,
    TOT_NON_CUR_LIAB,
    PAID_UP_CAPITAL,
    CAP_RSRV,
    UNDISTRIBUTED_PROFIT,
    OWNERS_EQUITY,
    OPDATE AS update_date
  FROM
    AINDEXFINANCIALDERIVATIVE
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  lazy val readFrom       = "AINDEXFINANCIALDERIVATIVE"
  lazy val saveTo         = "aindex_financial_derivative"
  lazy val readUpdateCol  = "OPDATE"
  lazy val saveUpdateCol  = "update_date"
  lazy val primaryKeyName = "PK_aindex_financial_derivative"
  lazy val primaryColumn  = Seq("object_id")
  lazy val index1         = ("IDX_aindex_financial_derivative", Seq("update_date"))
  lazy val index2         = ("IDX_aindex_financial_derivative", Seq("report_period", "symbol"))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(connMarket, query, connBizTable(saveTo))
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
          (primaryKeyName, primaryColumn),
          Seq(index1, index2)
        )
      case Command.SyncFromLastUpdate :: _ =>
        syncInsertFromLastUpdate(
          connMarketTableColumn(readFrom, readUpdateCol),
          connBizTableColumn(saveTo, saveUpdateCol),
          queryFromDate
        )
      case Command.OverrideFromLastUpdate :: _ =>
        syncUpsertFromLastUpdate(
          connMarketTableColumn(readFrom, readUpdateCol),
          connBizTableColumn(saveTo, saveUpdateCol),
          primaryColumn,
          queryFromDate
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          connMarket,
          queryFromDate(timeFrom),
          connBizTable(saveTo),
          primaryColumn
        )
      case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
        syncUpsert(
          connMarket,
          queryDateRange(timeFrom, timeTo),
          connBizTable(saveTo),
          primaryColumn
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
  }
}
