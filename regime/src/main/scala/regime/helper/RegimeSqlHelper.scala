package regime.helper

import scala.io.Source

object RegimeSqlHelper {
  def fromFile(filepath: String): String =
    Source.fromFile(filepath).mkString

  def fromResource(filename: String): String =
    Source.fromResource(filename).mkString
}
