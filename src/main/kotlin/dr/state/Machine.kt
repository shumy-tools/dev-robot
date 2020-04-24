package dr.state

import dr.base.User
import dr.ctx.Context
import dr.io.DEntity
import dr.schema.SEntity
import dr.schema.SMachine
import dr.spi.IQueryExecutor
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.createInstance

fun buildMachine(sEntity: SEntity): Machine<*, *> {
  val sMachine = sEntity.machine!!
  val instance = sMachine.clazz.createInstance()

  instance.sMachine = sMachine
  instance.stateQuery = Context.query("${sEntity.name} | @id == ?id | { @state }")

  return instance
}

open class Machine<S: Enum<*>, E: Any> {
  val user: User
    get() = Context.get().user

  lateinit var sMachine: SMachine
    internal  set

  lateinit var stateQuery: IQueryExecutor
    internal  set

  private val enter = hashMapOf<String, EnterActions.() -> Unit>()
  private val events = hashMapOf<KClass<out E>, MutableMap<String, After<out E>>>()

  internal fun include(evtType: KClass<out E>, from: S, after: After<out E>) {
    val state = from.toString()
    val eMap = events.getOrPut(evtType) { hashMapOf() }
    if (eMap.containsKey(state))
      throw Exception("Transition already exists! - ($evtType, $from)")

    eMap[state] = after
  }

  @Suppress("UNCHECKED_CAST")
  fun onEvent(id: Long, inEvt: Any) {
    val event = inEvt as E
    val state = stateQuery.exec("id" to id).getValue("@state") as String

    val evtType = event.javaClass.kotlin
    val states = events[evtType] ?: throw Exception("StateMachine event '$event' not found! - (${this.javaClass.kotlin.qualifiedName})")
    val then = states[state] ?: throw Exception("StateMachine transit '$event':'$state' not found! - (${this.javaClass.kotlin.qualifiedName})")
    then.onEvent?.invoke(EventActions(state, event))
  }

  fun onCreate(entity: DEntity) {
    //TODO: how to fire onEnter for the first state?
  }

  fun onUpdate(id: Long, entity: DEntity) {
    val state = stateQuery.exec("id" to id).getValue("@state") as String
    //TODO: what events to fire?
  }

  inner class History internal constructor(private val state: String, private val evtType: KClass<out E>? = null) {
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

    fun forAll() {
      // TODO: the property state for all
    }

    fun forRole(role: String) {
      // TODO: the property state for role
    }

    fun forUser(user: String) {
      // TODO: the property state for user
    }
  }

  open inner class EnterActions internal constructor(private val state: String, private val evtType: KClass<out E>? = null) {
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

  inner class EventActions<EX: E> internal constructor(private val state: String, val event: EX): EnterActions(state) {
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
    if (this.enter.containsKey(state.toString()))
      throw Exception("State enter already exists! - ($state)")

    this.enter[state.toString()] = enter
  }

  fun <EX: E> on(state: S, evtType: KClass<EX>): From<EX> {
    return From(state, evtType)
  }

  fun query(query: String): IQueryExecutor {
    return Context.query(query)
  }
}