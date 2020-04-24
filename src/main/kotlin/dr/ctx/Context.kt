package dr.ctx

import dr.DrServer
import dr.base.User
import dr.schema.SEntity
import dr.spi.IQueryExecutor

object Context {
  private val local = ThreadLocal<Session>()

  fun get(): Session = local.get()!!
  fun set(session: Session) = local.set(session)
  fun clear() = local.set(null)

  fun create(data: Any): Map<String, Any?> {
    val server = get().server
    val entity = server.processor.create(data)

    val instructions = server.translator.create(entity)
    server.mAdaptor.commit(instructions)

    return instructions.output
  }

  fun update(id: Long, type: SEntity, data: Map<String, Any?>): Map<String, Any?> {
    val server = get().server
    val entity = server.processor.update(type, data)

    val instructions = server.translator.update(id, entity)
    server.mAdaptor.commit(instructions)

    return instructions.output
  }

  fun query(query: String): IQueryExecutor {
    return get().server.qService.compile(query)
  }
}

val ANONYMOUS = User("anonymous", "no-email", emptyList())
class Session(val server: DrServer, val user: User = ANONYMOUS)