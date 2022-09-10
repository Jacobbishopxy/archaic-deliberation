package regime.market.finance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.market.{Command, Finance, RegimeTask}
import regime.market.Common.{connMarket, connBizTable}

object AShareIncome extends RegimeTask with Finance {
  lazy val query = """
  SELECT
    OBJECT_ID AS object_id,
    S_INFO_WINDCODE AS symbol,
    ANN_DT,
    REPORT_PERIOD,
    STATEMENT_TYPE,
    CRNCY_CODE,
    TOT_OPER_REV,
    OPER_REV,
    INT_INC,
    NET_INT_INC,
    INSUR_PREM_UNEARNED,
    HANDLING_CHRG_COMM_INC,
    NET_HANDLING_CHRG_COMM_INC,
    NET_INC_OTHER_OPS,
    PLUS_NET_INC_OTHER_BUS,
    PREM_INC,
    LESS_CEDED_OUT_PREM,
    CHG_UNEARNED_PREM_RES,
    INCL_REINSURANCE_PREM_INC,
    NET_INC_SEC_TRADING_BROK_BUS,
    NET_INC_SEC_UW_BUS,
    NET_INC_EC_ASSET_MGMT_BUS,
    OTHER_BUS_INC,
    PLUS_NET_GAIN_CHG_FV,
    PLUS_NET_INVEST_INC,
    INCL_INC_INVEST_ASSOC_JV_ENTP,
    PLUS_NET_GAIN_FX_TRANS,
    TOT_OPER_COST,
    LESS_OPER_COST,
    LESS_INT_EXP,
    LESS_HANDLING_CHRG_COMM_EXP,
    LESS_TAXES_SURCHARGES_OPS,
    LESS_SELLING_DIST_EXP,
    LESS_GERL_ADMIN_EXP,
    LESS_FIN_EXP,
    LESS_IMPAIR_LOSS_ASSETS,
    PREPAY_SURR,
    TOT_CLAIM_EXP,
    CHG_INSUR_CONT_RSRV,
    DVD_EXP_INSURED,
    REINSURANCE_EXP,
    OPER_EXP,
    LESS_CLAIM_RECB_REINSURER,
    LESS_INS_RSRV_RECB_REINSURER,
    LESS_EXP_RECB_REINSURER,
    OTHER_BUS_COST,
    OPER_PROFIT,
    PLUS_NON_OPER_REV,
    LESS_NON_OPER_EXP,
    IL_NET_LOSS_DISP_NONCUR_ASSET,
    TOT_PROFIT,
    INC_TAX,
    UNCONFIRMED_INVEST_LOSS,
    NET_PROFIT_INCL_MIN_INT_INC,
    NET_PROFIT_EXCL_MIN_INT_INC,
    MINORITY_INT_INC,
    OTHER_COMPREH_INC,
    TOT_COMPREH_INC,
    TOT_COMPREH_INC_PARENT_COMP,
    TOT_COMPREH_INC_MIN_SHRHLDR,
    EBIT,
    EBITDA,
    NET_PROFIT_AFTER_DED_NR_LP,
    NET_PROFIT_UNDER_INTL_ACC_STA,
    COMP_TYPE_CODE,
    S_FA_EPS_BASIC,
    S_FA_EPS_DILUTED,
    ACTUAL_ANN_DT,
    INSURANCE_EXPENSE,
    SPE_BAL_OPER_PROFIT,
    TOT_BAL_OPER_PROFIT,
    SPE_BAL_TOT_PROFIT,
    TOT_BAL_TOT_PROFIT,
    SPE_BAL_NET_PROFIT,
    TOT_BAL_NET_PROFIT,
    UNDISTRIBUTED_PROFIT,
    ADJLOSSGAIN_PREVYEAR,
    TRANSFER_FROM_SURPLUSRESERVE,
    TRANSFER_FROM_HOUSINGIMPREST,
    TRANSFER_FROM_OTHERS,
    DISTRIBUTABLE_PROFIT,
    WITHDR_LEGALSURPLUS,
    WITHDR_LEGALPUBWELFUNDS,
    WORKERS_WELFARE,
    WITHDR_BUZEXPWELFARE,
    WITHDR_RESERVEFUND,
    DISTRIBUTABLE_PROFIT_SHRHDER,
    PRFSHARE_DVD_PAYABLE,
    WITHDR_OTHERSURPRESERVE,
    COMSHARE_DVD_PAYABLE,
    CAPITALIZED_COMSTOCK_DIV,
    S_INFO_COMPCODE,
    NET_AFTER_DED_NR_LP_CORRECT,
    OTHER_INCOME,
    MEMO,
    ASSET_DISPOSAL_INCOME,
    CONTINUED_NET_PROFIT,
    END_NET_PROFIT,
    CREDIT_IMPAIRMENT_LOSS,
    NET_EXPOSURE_HEDGING_BENEFITS,
    RD_EXPENSE,
    STMNOTE_FINEXP,
    FIN_EXP_INT_INC,
    IS_CALCULATION,
    OTHER_IMPAIR_LOSS_ASSETS,
    OPDATE AS update_date
  FROM
    ASHAREINCOME
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  lazy val saveTo         = "ashare_income"
  lazy val primaryKeyName = "PK_ashare_income"
  lazy val primaryColumn  = Seq("object_id")
  lazy val indexName1     = "IDX_ashare_income_1"
  lazy val indexName2     = "IDX_ashare_income_2"
  lazy val indexColumn1   = Seq("update_date")
  lazy val indexColumn2   = Seq("report_period", "symbol")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(connMarket, query, connBizTable(saveTo))
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
          (primaryKeyName, primaryColumn),
          Seq((indexName1, indexColumn1), (indexName2, indexColumn2))
        )
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
          (primaryKeyName, primaryColumn),
          Seq((indexName1, indexColumn1), (indexName2, indexColumn2))
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
      case _ => throw new Exception("Invalid command")
    }
  }
}
