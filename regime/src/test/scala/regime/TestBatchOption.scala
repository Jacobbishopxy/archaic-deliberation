package regime

import regime.helper.BatchOption

object TestBatchOption extends App {

  val rowsOfTable  = 49
  val fetchSize    = 10
  val callingTimes = math.floor(rowsOfTable / fetchSize).toInt

  val foo = BatchOption.create("foo", true, fetchSize, callingTimes).get

  val ip = foo.genIterPagination()

  println(ip.next())
  println(ip.next())
  println(ip.next())
  println(ip.next())
  println(ip.next())
  println(ip.hasNext)

  /*
  Pagination(foo,true,10,0)
  Pagination(foo,true,10,10)
  Pagination(foo,true,10,20)
  Pagination(foo,true,10,30)
  Pagination(foo,true,10,40)
  false
   */
}
