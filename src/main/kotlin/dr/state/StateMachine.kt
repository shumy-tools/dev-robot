package dr.state

import dr.ctx.Context
import dr.spi.IQueryExecutor
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class StateMachine(val value: KClass<out Machine<*, *>>)

open class Machine<S, E> {
  lateinit var user: String
  lateinit var history: History<S, E>

  private val stateEnter = hashMapOf<S, Actions.() -> Unit>()

  private val events = hashMapOf<E, MutableMap<S, Then>>()
  internal fun set(event: E, from: S, then: Then) {
    val eMap = events.getOrPut(event) { hashMapOf() }
    if (eMap.containsKey(from))
      throw Exception("Transition already exists! - ($event, $from)")

    eMap[from] = then
  }

  class For {
    infix fun forRole(role: String) {

    }

    infix fun forUser(user: String) {

    }
  }

  class Actions {
    infix fun check(predicate: () -> Boolean) {

    }

    fun <T, R> open(prop: KProperty1<T, R>): For {
      return For()
    }

    fun <T, R> close(prop: KProperty1<T, R>): For {
      return For()
    }
  }

  inner class Then(val to: S, val users: Set<String>, val roles: Set<String>) {
    var execFun: (Actions.() -> Unit)? = null
      private set

    infix fun then(exec: Actions.() -> Unit) { execFun = exec }
  }

  inner class Transit(private val event: E, private val users: Set<String>, private val roles: Set<String>) {
    infix fun transit(pair: Pair<S, S>): Then {
      val then = Then(pair.second, users, roles)
      set(event, pair.first, then)
      return then
    }
  }

  inner class From(private val event: E) {
    infix fun fromRole(role: String): Transit {
      return Transit(event, emptySet(), setOf(role))
    }

    infix fun fromRoles(roles: Set<String>): Transit {
      return Transit(event, emptySet(), roles)
    }

    infix fun fromUser(user: String): Transit {
      return Transit(event, setOf(user), emptySet())
    }

    infix fun fromUsers(users: Set<String>): Transit {
      return Transit(event, users, emptySet())
    }

    infix fun transit(pair: Pair<S, S>): Then {
      val then = Then(pair.second, emptySet(), emptySet())
      set(event, pair.first, then)
      return then
    }
  }


  fun state(state: S, enter: Actions.() -> Unit) {
    if (stateEnter.containsKey(state))
      throw Exception("State enter already exists! - ($state)")

    stateEnter[state] = enter
  }

  fun on(event: E): From {
    return From(event)
  }

  fun query(query: String): IQueryExecutor {
    return Context.query(query)
  }
}


class History<S, E> {
  class Record {
    fun <T> get(key: String): T {
      return "" as T
    }
  }

  fun set(key: String, value: Any) {

  }

  fun last(event: E, transit: Pair<S, S>? = null): Record {
    return Record()
  }
}