package dr.base

import dr.schema.Link
import dr.schema.Master
import dr.schema.RefID

@Master
data class User(
  val name: String,
  val email: String,

  @Link(Role::class)
  val roles: List<RefID>
)

@Master
data class Role(val name: String)