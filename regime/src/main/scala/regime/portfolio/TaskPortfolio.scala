package regime.portfolio

import regime.TaskCategory
import regime.helper.RegimeSpark
import regime.portfolio._

trait Portfolio extends RegimeSpark {
  val appName: String = this.getClass.getSimpleName.dropRight(1)
}

object Portfolio extends TaskCategory {
  def unapply(str: String): Portfolio = str match {
    case RPortfolioNetValueChange.appName => RPortfolioNetValueChange
    case _                                => throw new Exception(s"$str is not in Portfolio")
  }
}
