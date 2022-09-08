package regime.market

object Command {
  val Initialize            = "Initialize"
  val SyncAll               = "SyncAll"
  val DailyUpsert           = "DailyUpsert"
  val DailyDelete           = "DailyDelete"
  val CatchFromLastUpdate   = "CatchFromLastUpdate"
  val TimeFromTillNowUpsert = "TimeFromTillNowUpsert"
  val TimeFromTillNowDelete = "TimeFromTillNowDelete"
  val TimeRangeUpsert       = "TimeRangeUpsert"
  val TimeRangeDelete       = "TimeRangeDelete"
  val ExecuteOnce           = "ExecuteOnce"
}
