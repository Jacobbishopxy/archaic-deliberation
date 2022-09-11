package regime.product

import regime.TaskCategory
import regime.helper.RegimeSpark

trait Product extends RegimeSpark {
  val appName: String = this.getClass.getSimpleName.dropRight(1)
}

object Product extends TaskCategory {
  def unapply(str: String): Product = str match {
    case _ => throw new Exception(s"$str is not in Product")
  }
}
