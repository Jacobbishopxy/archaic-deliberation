package regime.product

import regime.TaskCategory
import regime.helper.RegimeSpark
import regime.product._

trait Product extends RegimeSpark {
  val appName: String = this.getClass.getSimpleName.dropRight(1)
}

object Product extends TaskCategory {
  def unapply(str: String): Product = str match {
    case IProductInformation.appName => IProductInformation
    case IProductBalance.appName     => IProductBalance
    case IProductTransaction.appName => IProductTransaction
    case _                           => throw new Exception(s"$str is not in Product")
  }
}
