package dr.ctx

import dr.base.Role
import dr.base.User
import dr.io.InputService
import dr.io.Instructions
import dr.query.QueryService
import dr.schema.RefID
import dr.schema.SEntity
import dr.spi.IQueryExecutor
import dr.spi.IReadAccess
import java.util.concurrent.atomic.AtomicReference
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

object Context {
  // TODO: move cache to cluster?
  private val tRoles = AtomicReference<Map<Long, Role>>()
  private val tSession = ThreadLocal<Session>()

  var allRoles: Map<Long, Role>
    get() = tRoles.get()!!
    set(value) = tRoles.set(value)

  var session: Session
    get() = tSession.get()!!
    set(value) = tSession.set(value)

  val instructions: Instructions
    get() = session.instructions

  fun roles(select: List<RefID>) = select.mapNotNull { refID ->
    val role = allRoles[refID.id]
    role?.let { it.name to it }
  }.toMap()

  fun clear() = tSession.set(null)

  fun create(data: Any) = session.iService.create(data)
  fun update(id: Long, type: SEntity, data: Map<KProperty<Any>, Any?>) = session.iService.update(id, type, data)
  fun action(id: Long, type: SEntity, evt: Any) = session.iService.action(id, type, evt)

  @Suppress("UNCHECKED_CAST")
  fun <E: Any> query(type: KClass<E>, query: String, access: ((IReadAccess) -> Unit)? = null): IQueryExecutor<E> {
    val qReady = session.qService.compile("${type.qualifiedName} $query")
    access?.invoke(qReady.second)
    return qReady.first as IQueryExecutor<E>
  }
}

val ANONYMOUS = User("anonymous", "no-email", emptyList())
class Session(val iService: InputService, val qService: QueryService, val user: User = ANONYMOUS) {
  internal val vars = mutableMapOf<String, Any>()
  internal val instructions = Instructions()
}