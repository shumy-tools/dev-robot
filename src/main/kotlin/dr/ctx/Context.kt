package dr.ctx

import dr.DrServer
import dr.base.User
import dr.io.Instructions
import dr.schema.RefID
import dr.schema.SEntity
import dr.spi.IQueryExecutor
import dr.spi.IReadAccess

object Context {
  private val tSession = ThreadLocal<Session>()
  private val tInstructions = ThreadLocal<Instructions>()

  var session: Session
    get() = tSession.get()!!
    set(value) = tSession.set(value)

  var instructions: Instructions
    get() = tInstructions.get()!!
    set(value) = tInstructions.set(value)

  fun clear() {
    tSession.set(null)
    tInstructions.set(null)
  }

  fun create(data: Any): RefID {
    val server = session.server
    val entity = server.processor.create(data)

    val more = server.translator.create(entity)
    instructions.include(more)

    return more.root.refID
  }

  fun update(id: Long, type: SEntity, data: Map<String, Any?>) {
    val server = session.server
    val entity = server.processor.update(type, data)

    val more = server.translator.update(id, entity)
    instructions.include(more)
  }

  fun query(query: String, access: ((IReadAccess) -> Unit)? = null): IQueryExecutor {
    val qReady = session.server.qService.compile(query)
    access?.invoke(qReady.second)
    return qReady.first
  }
}

val ANONYMOUS = User("anonymous", "no-email", emptyList())
class Session(val server: DrServer, val user: User = ANONYMOUS)