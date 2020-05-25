package dr.base

import dr.schema.Master
import java.time.LocalDateTime

@Master
data class History(
  val ts: LocalDateTime,
  val user: String,

  val evtType: String?,
  val evt: String?,

  val from: String?,
  val to: String,

  val data: MutableMap<String, Any>
)