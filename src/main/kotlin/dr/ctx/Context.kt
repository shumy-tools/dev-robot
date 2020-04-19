package dr.ctx

import dr.DrServer
import dr.base.User
import dr.schema.SEntity
import dr.spi.IQueryExecutor
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

object Context {
  private val queryCache = ConcurrentHashMap<String, IQueryExecutor>()
  private val local = ThreadLocal<Session>()

  fun get(): Session = local.get()
  fun set(session: Session) = local.set(session)
  fun clear() = local.set(null)

  fun create(data: Any): Map<String, Any?> {
    val server = get().server
    val entity = server.processor.create(data)
    return server.mEngine.create(entity)
  }

  fun update(id: Long, type: SEntity, data: Map<String, Any?>): Map<String, Any?> {
    val server = get().server
    val entity = server.processor.update(type, data)
    return server.mEngine.update(entity, id)
  }

  fun query(query: String): IQueryExecutor {
    val digest = MessageDigest.getInstance("SHA-256")
    val hash = digest.digest(query.toByteArray()).toString()

    return queryCache.getOrPut(hash) {
      get().server.qEngine.compile(query)
    }
  }
}

class Session(val server: DrServer, val user: User? = null)