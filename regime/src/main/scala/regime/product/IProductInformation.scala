package regime.product

import org.apache.spark.sql.SparkSession

import regime.helper._
import regime.product.Common.{connProduct, connBizTable}

object IProductInformation extends RegimeSpark with Product {
  lazy val query = """
  SELECT
    productNum AS product_num,
    productId AS product_id,
    productName AS product_name,
    productFullName AS product_full_name,
    instituTionId AS institution_id,
    productQuota AS product_quota,
    NAV AS nav,
    productType AS product_type,
    openDate AS open_date,
    endDate AS end_date,
    currencyIdList AS currency_id_list,
    IPOCurrencyIdList AS ipo_currency_id_list,
    exchIdList AS exch_id_list,
    productStatus AS product_status,
    ReckoningFlag AS reckonging_flag,
    closeStkNavFlag AS close_stk_nav_flag,
    stkvalueType AS stk_value_type,
    transAddress AS trans_address,
    memo AS memo,
    inputTime AS input_time,
    optId AS opt_id,
    repoInterFlag AS repo_inter_flag,
    setupBegintime AS setup_begin_time,
    setupEndTime AS setup_end_time,
    closeBeginTime AS close_begin_time,
    closeEndTime AS close_end_time,
    TradeOrderFlag AS trade_order_flag,
    MessageOptIdList AS message_opt_id_list,
    isAuto AS is_auto,
    AcctCount AS acct_count,
    PortfolioCount AS portfolio_count,
    TradeInstituTionId AS trade_institution_id,
    PermitFlag AS permit_flag,
    OddAmt AS ood_amt,
    FileOrderFlag AS file_order_flag,
    CurrentAsset AS current_asset
  FROM
    bside_product
  """

  lazy val saveTo         = "iproduct_information"
  lazy val primaryKeyName = "PK_iproduct_information"
  lazy val primaryColumn  = Seq("product_num")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(connProduct, query, connBizTable(saveTo))
        createPrimaryKey(connBizTable(saveTo), primaryKeyName, primaryColumn)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKey(connBizTable(saveTo), primaryKeyName, primaryColumn)
      case Command.SyncAll :: _ =>
        syncReplaceAll(connProduct, query, connBizTable(saveTo))
      case _ =>
        throw new Exception("Invalid Command")
    }
  }
}
