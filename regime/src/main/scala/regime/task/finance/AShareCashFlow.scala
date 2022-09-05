package regime.task.finance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.task.Common.{connMarket, connBiz}
import regime.helper._
import regime.task.{Command, Finance}

object AShareCashFlow extends RegimeSpark with Finance {
  val appName: String = "AShareCashFlow"

  val query = """
  SELECT
    OBJECT_ID AS object_id,
    S_INFO_WINDCODE AS symbol,
    ANN_DT,
    REPORT_PERIOD,
    STATEMENT_TYPE,
    CRNCY_CODE,
    CASH_RECP_SG_AND_RS,
    RECP_TAX_RENDS,
    NET_INCR_DEP_COB,
    NET_INCR_LOANS_CENTRAL_BANK,
    NET_INCR_FUND_BORR_OFI,
    CASH_RECP_PREM_ORIG_INCO,
    NET_INCR_INSURED_DEP,
    NET_CASH_RECEIVED_REINSU_BUS,
    NET_INCR_DISP_TFA,
    NET_INCR_INT_HANDLING_CHRG,
    NET_INCR_DISP_FAAS,
    NET_INCR_LOANS_OTHER_BANK,
    NET_INCR_REPURCH_BUS_FUND,
    OTHER_CASH_RECP_RAL_OPER_ACT,
    STOT_CASH_INFLOWS_OPER_ACT,
    CASH_PAY_GOODS_PURCH_SERV_REC,
    CASH_PAY_BEH_EMPL,
    PAY_ALL_TYP_TAX,
    NET_INCR_CLIENTS_LOAN_ADV,
    NET_INCR_DEP_CBOB,
    CASH_PAY_CLAIMS_ORIG_INCO,
    HANDLING_CHRG_PAID,
    COMM_INSUR_PLCY_PAID,
    OTHER_CASH_PAY_RAL_OPER_ACT,
    STOT_CASH_OUTFLOWS_OPER_ACT,
    NET_CASH_FLOWS_OPER_ACT,
    CASH_RECP_DISP_WITHDRWL_INVEST,
    CASH_RECP_RETURN_INVEST,
    NET_CASH_RECP_DISP_FIOLTA,
    NET_CASH_RECP_DISP_SOBU,
    OTHER_CASH_RECP_RAL_INV_ACT,
    STOT_CASH_INFLOWS_INV_ACT,
    CASH_PAY_ACQ_CONST_FIOLTA,
    CASH_PAID_INVEST,
    NET_CASH_PAY_AQUIS_SOBU,
    OTHER_CASH_PAY_RAL_INV_ACT,
    NET_INCR_PLEDGE_LOAN,
    STOT_CASH_OUTFLOWS_INV_ACT,
    NET_CASH_FLOWS_INV_ACT,
    CASH_RECP_CAP_CONTRIB,
    INCL_CASH_REC_SAIMS,
    CASH_RECP_BORROW,
    PROC_ISSUE_BONDS,
    OTHER_CASH_RECP_RAL_FNC_ACT,
    STOT_CASH_INFLOWS_FNC_ACT,
    CASH_PREPAY_AMT_BORR,
    CASH_PAY_DIST_DPCP_INT_EXP,
    INCL_DVD_PROFIT_PAID_SC_MS,
    OTHER_CASH_PAY_RAL_FNC_ACT,
    STOT_CASH_OUTFLOWS_FNC_ACT,
    NET_CASH_FLOWS_FNC_ACT,
    EFF_FX_FLU_CASH,
    NET_INCR_CASH_CASH_EQU,
    CASH_CASH_EQU_BEG_PERIOD,
    CASH_CASH_EQU_END_PERIOD,
    NET_PROFIT,
    UNCONFIRMED_INVEST_LOSS,
    PLUS_PROV_DEPR_ASSETS,
    DEPR_FA_COGA_DPBA,
    AMORT_INTANG_ASSETS,
    AMORT_LT_DEFERRED_EXP,
    DECR_DEFERRED_EXP,
    INCR_ACC_EXP,
    LOSS_DISP_FIOLTA,
    LOSS_SCR_FA,
    LOSS_FV_CHG,
    FIN_EXP,
    INVEST_LOSS,
    DECR_DEFERRED_INC_TAX_ASSETS,
    INCR_DEFERRED_INC_TAX_LIAB,
    DECR_INVENTORIES,
    DECR_OPER_PAYABLE,
    INCR_OPER_PAYABLE,
    OTHERS,
    IM_NET_CASH_FLOWS_OPER_ACT,
    CONV_DEBT_INTO_CAP,
    CONV_CORP_BONDS_DUE_WITHIN_1Y,
    FA_FNC_LEASES,
    END_BAL_CASH,
    LESS_BEG_BAL_CASH,
    PLUS_END_BAL_CASH_EQU,
    LESS_BEG_BAL_CASH_EQU,
    IM_NET_INCR_CASH_CASH_EQU,
    FREE_CASH_FLOW,
    COMP_TYPE_CODE,
    ACTUAL_ANN_DT,
    SPE_BAL_CASH_INFLOWS_OPER,
    TOT_BAL_CASH_INFLOWS_OPER,
    SPE_BAL_CASH_OUTFLOWS_OPER,
    TOT_BAL_CASH_OUTFLOWS_OPER,
    TOT_BAL_NETCASH_OUTFLOWS_OPER,
    SPE_BAL_CASH_INFLOWS_INV,
    TOT_BAL_CASH_INFLOWS_INV,
    SPE_BAL_CASH_OUTFLOWS_INV,
    TOT_BAL_CASH_OUTFLOWS_INV,
    TOT_BAL_NETCASH_OUTFLOWS_INV,
    SPE_BAL_CASH_INFLOWS_FNC,
    TOT_BAL_CASH_INFLOWS_FNC,
    SPE_BAL_CASH_OUTFLOWS_FNC,
    TOT_BAL_CASH_OUTFLOWS_FNC,
    TOT_BAL_NETCASH_OUTFLOWS_FNC,
    SPE_BAL_NETCASH_INC,
    TOT_BAL_NETCASH_INC,
    SPE_BAL_NETCASH_EQU_UNDIR,
    TOT_BAL_NETCASH_EQU_UNDIR,
    SPE_BAL_NETCASH_INC_UNDIR,
    TOT_BAL_NETCASH_INC_UNDIR,
    S_INFO_COMPCODE,
    S_DISMANTLE_CAPITAL_ADD_NET,
    IS_CALCULATION,
    SECURITIE_NETCASH_RECEIVED,
    OPDATE AS update_date
  FROM
    ASHARECASHFLOW
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  val saveTo         = "ashare_cashflow"
  val primaryKeyName = "PK_ashare_cashflow"
  val primaryColumn  = Seq("object_id")
  val indexName1     = "IDX_ashare_cashflow_1"
  val indexName2     = "IDX_ashare_cashflow_2"
  val indexColumn1   = Seq("update_date")
  val indexColumn2   = Seq("report_period", "symbol")

  def process(spark: SparkSession, args: String*): Unit = {
    args.toList match {
      case Command.SyncAll :: _                    => syncAll(spark)
      case Command.ExecuteOnce :: _                => createPrimaryKeyAndIndex()
      case Command.TimeFromUpsert :: timeFrom :: _ => upsertFromDate(spark, timeFrom)
      case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
        upsertDateRange(spark, timeFrom, timeTo)
      case _ => throw new Exception("Invalid command")
    }
  }

  private def syncAll(spark: SparkSession): Unit = {
    val df = RegimeJdbcHelper(connMarket).readTable(spark, query)

    RegimeJdbcHelper(connBiz).saveTable(df, saveTo, SaveMode.Overwrite)
  }

  private def createPrimaryKeyAndIndex(): Unit = {
    val helper = RegimeJdbcHelper(connBiz)

    helper.createPrimaryKey(saveTo, primaryKeyName, primaryColumn)

    helper.createIndex(saveTo, indexName1, indexColumn1)
    helper.createIndex(saveTo, indexName2, indexColumn2)
  }

  private def upsertFromDate(spark: SparkSession, fromDate: String): Unit = {
    val df = RegimeJdbcHelper(connMarket).readTable(spark, queryFromDate(fromDate))

    RegimeJdbcHelper(connBiz).upsertTable(df, saveTo, None, false, primaryColumn, "doUpdate")
  }

  private def upsertDateRange(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    val df = RegimeJdbcHelper(connMarket).readTable(spark, queryDateRange(fromDate, toDate))

    RegimeJdbcHelper(connBiz).upsertTable(df, saveTo, None, false, primaryColumn, "doUpdate")
  }

}
