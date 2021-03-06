package dr.base

import dr.ctx.Context
import dr.schema.*

const val ANY = "@any" // defines any role
const val ROLES = "roles"
const val USERS = "users"

@Master
data class User(
  @Unique val name: String,
  val email: String,

  @Link(Role::class) val roles: List<RefID>
) {
  fun rolesMap(): Map<String, Role> = Context.roles(roles)
}

@Master
data class Role(@Unique val name: String)

fun loadRoles() {
  val qRoles = Context.query(Role::class,"{ * }").exec()
  Context.allRoles = qRoles.map { it.get<Long>(ID)!! to Role(it.get(Role::name)!!) }.toMap()
}