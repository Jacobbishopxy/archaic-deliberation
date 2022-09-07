package regime.market.finance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, Finance, RegimeTask}
import regime.market.Common.{connMarket, connBiz}

object AShareBalanceSheet extends RegimeTask with Finance {

  val appName: String = "AShareBalanceSheet"

  val query = """
  SELECT
    OBJECT_ID	AS object_id,
    S_INFO_WINDCODE as symbol,
    ANN_DT,
    REPORT_PERIOD,
    STATEMENT_TYPE,
    CRNCY_CODE,
    MONETARY_CAP,
    TRADABLE_FIN_ASSETS,
    NOTES_RCV,
    ACCT_RCV,
    OTH_RCV,
    PREPAY,
    DVD_RCV,
    INT_RCV,
    INVENTORIES,
    CONSUMPTIVE_BIO_ASSETS,
    DEFERRED_EXP,
    NON_CUR_ASSETS_DUE_WITHIN_1Y,
    SETTLE_RSRV,
    LOANS_TO_OTH_BANKS,
    PREM_RCV,
    RCV_FROM_REINSURER,
    RCV_FROM_CEDED_INSUR_CONT_RSRV,
    RED_MONETARY_CAP_FOR_SALE,
    OTH_CUR_ASSETS,
    TOT_CUR_ASSETS,
    FIN_ASSETS_AVAIL_FOR_SALE,
    HELD_TO_MTY_INVEST,
    LONG_TERM_EQY_INVEST,
    INVEST_REAL_ESTATE,
    TIME_DEPOSITS,
    OTH_ASSETS,
    LONG_TERM_REC,
    FIX_ASSETS,
    CONST_IN_PROG,
    PROJ_MATL,
    FIX_ASSETS_DISP,
    PRODUCTIVE_BIO_ASSETS,
    OIL_AND_NATURAL_GAS_ASSETS,
    INTANG_ASSETS,
    R_AND_D_COSTS,
    GOODWILL,
    LONG_TERM_DEFERRED_EXP,
    DEFERRED_TAX_ASSETS,
    LOANS_AND_ADV_GRANTED,
    OTH_NON_CUR_ASSETS,
    TOT_NON_CUR_ASSETS,
    CASH_DEPOSITS_CENTRAL_BANK,
    ASSET_DEP_OTH_BANKS_FIN_INST,
    PRECIOUS_METALS,
    DERIVATIVE_FIN_ASSETS,
    AGENCY_BUS_ASSETS,
    SUBR_REC,
    RCV_CEDED_UNEARNED_PREM_RSRV,
    RCV_CEDED_CLAIM_RSRV,
    RCV_CEDED_LIFE_INSUR_RSRV,
    RCV_CEDED_LT_HEALTH_INSUR_RSRV,
    MRGN_PAID,
    INSURED_PLEDGE_LOAN,
    CAP_MRGN_PAID,
    INDEPENDENT_ACCT_ASSETS,
    CLIENTS_CAP_DEPOSIT,
    CLIENTS_RSRV_SETTLE,
    INCL_SEAT_FEES_EXCHANGE,
    RCV_INVEST,
    TOT_ASSETS,
    ST_BORROW,
    BORROW_CENTRAL_BANK,
    DEPOSIT_RECEIVED_IB_DEPOSITS,
    LOANS_OTH_BANKS,
    TRADABLE_FIN_LIAB,
    NOTES_PAYABLE,
    ACCT_PAYABLE,
    ADV_FROM_CUST,
    FUND_SALES_FIN_ASSETS_RP,
    HANDLING_CHARGES_COMM_PAYABLE,
    EMPL_BEN_PAYABLE,
    TAXES_SURCHARGES_PAYABLE,
    INT_PAYABLE,
    DVD_PAYABLE,
    OTH_PAYABLE,
    ACC_EXP,
    DEFERRED_INC,
    ST_BONDS_PAYABLE,
    PAYABLE_TO_REINSURER,
    RSRV_INSUR_CONT,
    ACTING_TRADING_SEC,
    ACTING_UW_SEC,
    NON_CUR_LIAB_DUE_WITHIN_1Y,
    OTH_CUR_LIAB,
    TOT_CUR_LIAB,
    LT_BORROW,
    BONDS_PAYABLE,
    LT_PAYABLE,
    SPECIFIC_ITEM_PAYABLE,
    PROVISIONS,
    DEFERRED_TAX_LIAB,
    DEFERRED_INC_NON_CUR_LIAB,
    OTH_NON_CUR_LIAB,
    TOT_NON_CUR_LIAB,
    LIAB_DEP_OTH_BANKS_FIN_INST,
    DERIVATIVE_FIN_LIAB,
    CUST_BANK_DEP,
    AGENCY_BUS_LIAB,
    OTH_LIAB,
    PREM_RECEIVED_ADV,
    DEPOSIT_RECEIVED,
    INSURED_DEPOSIT_INVEST,
    UNEARNED_PREM_RSRV,
    OUT_LOSS_RSRV,
    LIFE_INSUR_RSRV,
    LT_HEALTH_INSUR_V,
    INDEPENDENT_ACCT_LIAB,
    INCL_PLEDGE_LOAN,
    CLAIMS_PAYABLE,
    DVD_PAYABLE_INSURED,
    TOT_LIAB,
    CAP_STK,
    CAP_RSRV,
    SPECIAL_RSRV,
    SURPLUS_RSRV,
    UNDISTRIBUTED_PROFIT,
    LESS_TSY_STK,
    PROV_NOM_RISKS,
    CNVD_DIFF_FOREIGN_CURR_STAT,
    UNCONFIRMED_INVEST_LOSS,
    MINORITY_INT,
    TOT_SHRHLDR_EQY_EXCL_MIN_INT,
    TOT_SHRHLDR_EQY_INCL_MIN_INT,
    TOT_LIAB_SHRHLDR_EQY,
    COMP_TYPE_CODE,
    ACTUAL_ANN_DT,
    SPE_CUR_ASSETS_DIFF,
    TOT_CUR_ASSETS_DIFF,
    SPE_NON_CUR_ASSETS_DIFF,
    TOT_NON_CUR_ASSETS_DIFF,
    SPE_BAL_ASSETS_DIFF,
    TOT_BAL_ASSETS_DIFF,
    SPE_CUR_LIAB_DIFF,
    TOT_CUR_LIAB_DIFF,
    SPE_NON_CUR_LIAB_DIFF,
    TOT_NON_CUR_LIAB_DIFF,
    SPE_BAL_LIAB_DIFF,
    TOT_BAL_LIAB_DIFF,
    SPE_BAL_SHRHLDR_EQY_DIFF,
    TOT_BAL_SHRHLDR_EQY_DIFF,
    SPE_BAL_LIAB_EQY_DIFF,
    TOT_BAL_LIAB_EQY_DIFF,
    LT_PAYROLL_PAYABLE,
    OTHER_COMP_INCOME,
    OTHER_EQUITY_TOOLS,
    OTHER_EQUITY_TOOLS_P_SHR,
    LENDING_FUNDS,
    ACCOUNTS_RECEIVABLE,
    ST_FINANCING_PAYABLE,
    PAYABLES,
    S_INFO_COMPCODE,
    TOT_SHR,
    HFS_ASSETS,
    HFS_SALES,
    FIN_ASSETS_COST_SHARING,
    FIN_ASSETS_FAIR_VALUE,
    CONTRACTUAL_ASSETS,
    CONTRACT_LIABILITIES,
    ACCOUNTS_RECEIVABLE_BILL,
    ACCOUNTS_PAYABLE,
    OTH_RCV_TOT,
    STM_BS_TOT,
    CONST_IN_PROG_TOT,
    OTH_PAYABLE_TOT,
    LT_PAYABLE_TOT,
    DEBT_INVESTMENT,
    OTHER_DEBT_INVESTMENT,
    OTHER_EQUITY_INVESTMENT,
    OTHER_ILLIQUIDFINANCIAL_ASSETS,
    OTHER_SUSTAINABLE_BOND,
    RECEIVABLES_FINANCING,
    RIGHT_USE_ASSETS,
    LEASE_LIAB,
    OPDATE as update_date
  FROM
    ASHAREBALANCESHEET
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  val saveTo         = "ashare_balance_sheet"
  val primaryKeyName = "PK_ashare_balance_sheet"
  val primaryColumn  = Seq("object_id")
  val indexName1     = "IDX_ashare_balance_sheet_1"
  val indexName2     = "IDX_ashare_balance_sheet_2"
  val indexColumn1   = Seq("update_date")
  val indexColumn2   = Seq("report_period", "symbol")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.SyncAll :: _ =>
        syncAll(connMarket, query, connBiz, saveTo)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBiz,
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq((indexName1, indexColumn1), (indexName2, indexColumn2))
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
      case _ => throw new Exception("Invalid command")
    }
  }
}
