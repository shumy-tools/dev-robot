package dr.ctx

import dr.base.User
import dr.io.InputService
import dr.io.Instructions
import dr.query.QueryService
import dr.schema.RefID
import dr.schema.SEntity
import dr.spi.IQueryExecutor
import dr.spi.IReadAccess
import kotlin.reflect.KProperty

object Context {
  private val tRefID = ThreadLocal<RefID>()
  private val tSession = ThreadLocal<Session>()

  var refID: RefID
    get() = tRefID.get()!!
    set(value) = tRefID.set(value)

  var session: Session
    get() = tSession.get()!!
    set(value) = tSession.set(value)

  val instructions: Instructions
    get() = session.instructions

  fun clear() {
    tSession.set(null)
  }

  fun create(data: Any) = session.iService.create(data)

  fun update(id: Long, type: SEntity, data: Map<KProperty<Any>, Any?>) = session.iService.update(id, type, data)

  fun query(query: String, access: ((IReadAccess) -> Unit)? = null): IQueryExecutor {
    val qReady = session.qService.compile(query)
    access?.invoke(qReady.second)
    return qReady.first
  }
}

val ANONYMOUS = User("anonymous", "no-email", emptyList())
class Session(val iService: InputService, val qService: QueryService, val user: User = ANONYMOUS) {
  val instructions = Instructions()
}