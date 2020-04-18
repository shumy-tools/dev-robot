package dr.base

import dr.schema.Link
import dr.schema.Master

@Master
data class User(
  val name: String,
  val email: String,

  @Link(Role::class)
  val roles: List<Long>
)

@Master
data class Role(val name: String)