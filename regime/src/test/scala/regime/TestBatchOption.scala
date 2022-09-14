package regime

import regime.helper.BatchOption

object TestBatchOption extends App {

  val foo = BatchOption.create("foo", true, 0, 10, 5).get

  val ip = foo.genIterPagination()

  println(ip.next())
  println(ip.next())
  println(ip.next())
  println(ip.next())
  println(ip.next())
  println(ip.next())
  println(ip.hasNext)

  /*
  Pagination(foo,true,0,10)
  Pagination(foo,true,10,20)
  Pagination(foo,true,20,30)
  Pagination(foo,true,30,40)
  Pagination(foo,true,40,50)
  Pagination(foo,true,50,60)
  false
   */
}
