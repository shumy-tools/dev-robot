package dr.base

import dr.schema.Master
import java.time.LocalDateTime

@Master
data class History(
  val ts: LocalDateTime,
  val state: String
)