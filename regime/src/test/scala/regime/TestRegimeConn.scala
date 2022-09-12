package regime

import regime.market.Common.{connMarket, connBiz}
import regime.product.Common.connProduct

object TestRegimeConn extends App {

  println("connMarket:")
  println(connMarket.options)

  println("connBiz:")
  println(connBiz.options)

  println("connProduct:")
  println(connProduct.options)

}
