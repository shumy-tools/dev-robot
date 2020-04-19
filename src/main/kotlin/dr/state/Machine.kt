package dr.state

import dr.base.User
import dr.ctx.Context
import dr.spi.IQueryExecutor
import kotlin.reflect.KProperty1

open class Machine<S, E> {
  val user: User
    get() = Context.get().user

  private val enter = hashMapOf<S, Actions<S, E>.() -> Unit>()
  private val events = hashMapOf<E, MutableMap<S, Then>>()

  internal fun include(event: E, from: S, then: Then) {
    val eMap = events.getOrPut(event) { hashMapOf() }
    if (eMap.containsKey(from))
      throw Exception("Transition already exists! - ($event, $from)")

    eMap[from] = then
  }

  internal fun call(event: E, state: S) {
    val states = events[event] ?: throw Exception("StateMachine event '$event' not found! - (${this.javaClass.kotlin.qualifiedName})")
    val then = states[state] ?: throw Exception("StateMachine transit '$event':'$state' not found! - (${this.javaClass.kotlin.qualifiedName})")
    then.onEvent?.invoke(Actions(state, event))
  }

  internal fun call(state: S) {
    val onEnter = enter[state] ?: throw Exception("StateMachine enter '$state' not found! - (${this.javaClass.kotlin.qualifiedName})")
    onEnter(Actions(state))
  }


  class For(private val action: Action, private val prop: KProperty1<*, *>) {
    enum class Action { OPEN, CLOSE }

    infix fun forRole(role: String) {

    }

    infix fun forUser(user: String) {

    }
  }

  inner class Actions<S, E>(private val state: S, private val event: E? = null) {
    val history = History(state, event)

    var checkOrder = 0
    infix fun check(predicate: () -> Boolean) {
      checkOrder++
      if (!predicate()) {
        val exText = if(event != null) "$event -> $state" else "enter $state"
        throw Exception("State machine failed on check constraint! - ($exText, check $checkOrder)")
      }
    }

    fun <T, R> open(prop: KProperty1<T, R>): For {
      return For(For.Action.OPEN, prop)
    }

    fun <T, R> close(prop: KProperty1<T, R>): For {
      return For(For.Action.CLOSE, prop)
    }
  }

  inner class Then(private val to: S, private val users: Set<String>, private val roles: Set<String>) {
    var onEvent: (Actions<S, E>.() -> Unit)? = null
      private set

    infix fun then(exec: Actions<S, E>.() -> Unit) { onEvent = exec }
  }

  inner class Transit(private val event: E, private val users: Set<String>, private val roles: Set<String>) {
    infix fun transit(pair: Pair<S, S>): Then {
      val then = Then(pair.second, users, roles)
      include(event, pair.first, then)
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
      include(event, pair.first, then)
      return then
    }
  }


  fun state(state: S, enter: Actions<S, E>.() -> Unit) {
    if (this.enter.containsKey(state))
      throw Exception("State enter already exists! - ($state)")

    this.enter[state] = enter
  }

  fun on(event: E): From {
    return From(event)
  }

  fun query(query: String): IQueryExecutor {
    return Context.query(query)
  }
}

class History<S, E>(val state: S, val event: E? = null) {
  class Record(private val data: Map<String, Any>) {
    @Suppress("UNCHECKED_CAST")
    fun <T> get(key: String): T {
      return data.getValue(key) as T
    }
  }

  fun set(key: String, value: Any) {
    // TODO: set value to commit ('event' -> 'state' or enter 'state')
  }

  fun last(event: E, transit: Pair<S, S>? = null): Record {
    // TODO: query from history
    return Record(emptyMap())
  }
}