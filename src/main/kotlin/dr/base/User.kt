package dr.base

import dr.ctx.Context
import dr.schema.Link
import dr.schema.Master
import dr.schema.RefID
import dr.schema.tabular.ID

const val ANY = "@any" // defines any role

@Master
data class User(
  val name: String,
  val email: String,

  @Link(Role::class)
  val roles: List<RefID>
) {
  fun rolesMap(): Map<String, Role> = Context.roles(roles)
}

@Master
data class Role(val name: String)

fun loadRoles() {
  val qRoles = Context.query(Role::class,"{ * }").exec()
  Context.allRoles = qRoles.map { it.get<Long>(ID)!! to Role(it.get(Role::name)!!) }.toMap()
}