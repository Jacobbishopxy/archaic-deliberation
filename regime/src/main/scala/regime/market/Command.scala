package regime.market

object Command {
  val Initialize             = "Initialize"
  val SyncAll                = "SyncAll"
  val DailyUpsert            = "DailyUpsert"
  val DailyDelete            = "DailyDelete"
  val SyncFromLastUpdate     = "SyncFromLastUpdate"
  val OverrideFromLastUpdate = "OverrideFromLastUpdate"
  val TimeFromTillNowUpsert  = "TimeFromTillNowUpsert"
  val TimeFromTillNowDelete  = "TimeFromTillNowDelete"
  val TimeRangeUpsert        = "TimeRangeUpsert"
  val TimeRangeDelete        = "TimeRangeDelete"
  val ExecuteOnce            = "ExecuteOnce"
}
