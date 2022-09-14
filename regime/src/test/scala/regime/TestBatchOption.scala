package regime

import regime.helper.BatchOption

object TestBatchOption extends App {

  val foo = BatchOption.create("foo", 0, 10, 5).get

  val ip = foo.genIterPagination()

  println(ip.next())
  println(ip.next())
  println(ip.next())
  println(ip.next())
  println(ip.hasNext)

  /*
  Pagination(foo,10,20)
  Pagination(foo,20,30)
  Pagination(foo,30,40)
  Pagination(foo,40,50)
  false
   */
}
