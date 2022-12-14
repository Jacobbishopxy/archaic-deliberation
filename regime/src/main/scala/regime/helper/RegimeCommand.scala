package regime.helper

object Command {
  val Initialize             = "Initialize"
  val Truncate               = "Truncate"
  val SyncAll                = "SyncAll"
  val ScanAll                = "ScanAll"
  val DailyUpsert            = "DailyUpsert"
  val DailyDelete            = "DailyDelete"
  val DailyReplace           = "DailyReplace"
  val SyncFromLastUpdate     = "SyncFromLastUpdate"
  val OverrideFromLastUpdate = "OverrideFromLastUpdate"
  val TimeFromTillNowUpsert  = "TimeFromTillNowUpsert"
  val TimeFromTillNowDelete  = "TimeFromTillNowDelete"
  val TimeRangeUpsert        = "TimeRangeUpsert"
  val TimeRangeDelete        = "TimeRangeDelete"
  val ExecuteOnce            = "ExecuteOnce"
}
