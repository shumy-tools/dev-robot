package dr.state

import dr.base.User
import dr.ctx.Context
import dr.spi.IQueryExecutor
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

open class Machine<S: Enum<*>, E: Any> {
  val user: User
    get() = Context.get().user

  private val enter = hashMapOf<S, EnterActions.() -> Unit>()
  private val events = hashMapOf<KClass<out E>, MutableMap<S, After<out E>>>()

  internal fun include(evtType: KClass<out E>, from: S, after: After<out E>) {
    val eMap = events.getOrPut(evtType) { hashMapOf() }
    if (eMap.containsKey(from))
      throw Exception("Transition already exists! - ($evtType, $from)")

    eMap[from] = after
  }

  internal fun onEvent(event: E, state: S) {
    val evtType = event.javaClass.kotlin
    val states = events[evtType] ?: throw Exception("StateMachine event '$event' not found! - (${this.javaClass.kotlin.qualifiedName})")
    val then = states[state] ?: throw Exception("StateMachine transit '$event':'$state' not found! - (${this.javaClass.kotlin.qualifiedName})")
    then.onEvent?.invoke(EventActions(state, event))
  }

  internal fun onEnter(state: S) {
    val onEnter = enter[state] ?: throw Exception("StateMachine enter '$state' not found! - (${this.javaClass.kotlin.qualifiedName})")
    onEnter(EnterActions(state))
  }

  inner class History internal constructor(private val state: S, private val evtType: KClass<out E>? = null) {
    inner class Record<EX: E>(val event: EX, private val data: Map<String, Any>) {
      @Suppress("UNCHECKED_CAST")
      fun <T> get(key: String): T {
        return data.getValue(key) as T
      }
    }

    fun set(key: String, value: Any) {
      // TODO: set value to commit ('event' -> 'state' or enter 'state')
    }

    fun <EX: E> last(evtType: KClass<EX>, transit: Pair<S, S>? = null): Record<EX> {
      // TODO: query from history
      TODO()
    }
  }

  class For internal constructor(private val state: PropertyState, private val prop: KProperty1<*, *>) {
    enum class PropertyState { OPEN, CLOSE }

    infix fun forRole(role: String) {
      // TODO: the property state for role
    }

    infix fun forUser(user: String) {
      // TODO: the property state for user
    }
  }

  open inner class EnterActions internal constructor(private val state: S, private val evtType: KClass<out E>? = null) {
    val history = History(state, evtType)

    fun <T, R> open(prop: KProperty1<T, R>): For {
      return For(For.PropertyState.OPEN, prop)
    }

    fun <T, R> close(prop: KProperty1<T, R>): For {
      return For(For.PropertyState.CLOSE, prop)
    }

    internal var checkOrder = 0
    open infix fun check(predicate: () -> Boolean) {
      checkOrder++
      if (!predicate())
        throw Exception("State machine failed on check constraint! - (enter $state, check $checkOrder)")
    }
  }

  inner class EventActions<EX: E> internal constructor(private val state: S, val event: EX): EnterActions(state) {
    override infix fun check(predicate: () -> Boolean) {
      checkOrder++
      if (!predicate())
        throw Exception("State machine failed on check constraint! - ($event -> $state, check $checkOrder)")
    }
  }

  inner class After<EX: E> internal constructor(private val to: S, private val users: Set<String>, private val roles: Set<String>) {
    var onEvent: (EventActions<out E>.() -> Unit)? = null
      private set

    @Suppress("UNCHECKED_CAST")
    infix fun after(exec: EventActions<EX>.() -> Unit) { onEvent = exec as EventActions<out E>.() -> Unit }
  }

  inner class Transit<EX: E> internal constructor(private val from: S, private val event: KClass<EX>, private val users: Set<String>, private val roles: Set<String>) {
    infix fun goto(to: S): After<EX> {
      val then = After<EX>(to, users, roles)
      include(event, from, then)
      return then
    }
  }

  inner class From<EX: E> internal constructor(private val from: S, private val evtType: KClass<EX>) {
    infix fun fromRole(role: String): Transit<EX> {
      return Transit(from, evtType, emptySet(), setOf(role))
    }

    infix fun fromRoles(roles: Set<String>): Transit<EX> {
      return Transit(from, evtType, emptySet(), roles)
    }

    infix fun fromUser(user: String): Transit<EX> {
      return Transit(from, evtType, setOf(user), emptySet())
    }

    infix fun fromUsers(users: Set<String>): Transit<EX> {
      return Transit(from, evtType, users, emptySet())
    }

    infix fun goto(to: S): After<EX> {
      val then = After<EX>(to, emptySet(), emptySet())
      include(evtType, from, then)
      return then
    }
  }

  fun enter(state: S, enter: EnterActions.() -> Unit) {
    if (this.enter.containsKey(state))
      throw Exception("State enter already exists! - ($state)")

    this.enter[state] = enter
  }

  fun <EX: E> on(state: S, evtType: KClass<EX>): From<EX> {
    return From(state, evtType)
  }

  fun query(query: String): IQueryExecutor {
    return Context.query(query)
  }
}