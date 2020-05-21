package dr.ctx

import dr.base.User
import dr.io.InputProcessor
import dr.io.InstructionBuilder
import dr.io.Instructions
import dr.query.QueryService
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
    val entity = session.processor.create(data)

    val more = session.translator.create(entity)
    instructions.include(more)

    return more.root.refID
  }

  fun update(id: Long, type: SEntity, data: Map<String, Any?>) {
    val entity = session.processor.update(type, id, data)

    val more = session.translator.update(id, entity)
    instructions.include(more)
  }

  fun query(query: String, access: ((IReadAccess) -> Unit)? = null): IQueryExecutor {
    val qReady = session.qService.compile(query)
    access?.invoke(qReady.second)
    return qReady.first
  }
}

val ANONYMOUS = User("anonymous", "no-email", emptyList())
class Session(val processor: InputProcessor, val translator: InstructionBuilder, val qService: QueryService, val user: User = ANONYMOUS)