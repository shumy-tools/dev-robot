package dr.state

import dr.base.User
import dr.ctx.Context
import dr.io.DEntity
import dr.JsonParser
import dr.query.QueryService
import dr.schema.RefID
import dr.schema.SEntity
import dr.schema.SMachine
import dr.schema.Schema
import dr.schema.tabular.ID
import dr.schema.tabular.STATE
import dr.spi.IQueryExecutor
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1

class UEntity<T: Any>(val id: Long, val data: MutableMap<String, Any?>) {
  @Suppress("UNCHECKED_CAST")
  fun <R: Any?> get(prop: KProperty1<T, R>): R {
    return data[prop.name] as R
  }

  fun <R: Any?> set(prop: KProperty1<T, R>, value: R) {
    data[prop.name] = value
  }

  override fun toString(): String {
    return JsonParser.write(data)
  }
}

open class Machine<T: Any, S: Enum<*>, E: Any> {
  val user: User
    get() = Context.session.user

  var onCreate: ((RefID, T) -> Unit)? = null
  var onUpdate: ((UEntity<T>) -> Unit)? = null

  private val enter = hashMapOf<String, EnterActions.() -> Unit>()
  private val events = hashMapOf<KClass<out E>, MutableMap<String, After<out E>>>()

  private lateinit var schema: Schema
  internal lateinit var sMachine: SMachine
  private lateinit var stateQuery: IQueryExecutor

  internal fun init(allSchema: Schema, sEntity: SEntity, qService: QueryService) {
    schema = allSchema
    sMachine = sEntity.machine!!
    stateQuery = qService.compile("${sEntity.name} | $ID == ?id | { $STATE }").first
  }

  internal fun include(evtType: KClass<out E>, from: S, after: After<out E>) {
    val state = from.toString()
    val eMap = events.getOrPut(evtType) { hashMapOf() }
    if (eMap.containsKey(state))
      throw Exception("Transition already exists! - ($evtType, $from)")

    eMap[state] = after
  }

  @Suppress("UNCHECKED_CAST")
  internal fun fireEvent(id: Long, inEvt: Any) {
    Context.refID = RefID(id)
      val event = inEvt as E
      val state = stateQuery.exec("id" to id).get<String>(STATE)

      val states = events.getValue(event.javaClass.kotlin)
      val then = states[state] ?: throw Exception("StateMachine transit '$event':'$state' not found! - (${javaClass.kotlin.qualifiedName})")
      then.onEvent?.invoke(EventActions(state, event))
    Context.refID = RefID()
  }

  @Suppress("UNCHECKED_CAST")
  internal fun fireCreate(entity: DEntity) {
    Context.refID = entity.refID
      onCreate?.invoke(entity.refID, entity.cEntity as T)
      val enter = enter.getValue(entity.state)
      enter.invoke(EnterActions(entity.state))
    Context.refID = RefID()
  }

  internal fun fireUpdate(entity: DEntity) {
    Context.refID = entity.refID
      onUpdate?.invoke(UEntity(entity.refID.id!!, entity.mEntity!!))
    Context.refID = RefID()
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

  class For internal constructor(private val state: PropertyState, private val prop: KProperty<*>) {
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

    fun open(prop: KProperty1<T, Any>): For {
      return For(For.PropertyState.OPEN, prop)
    }

    fun close(prop: KProperty1<T, Any>): For {
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

  fun query(query: String) = Context.query(query)

  fun create(entity: Any) = Context.create(entity)

  fun update(id: Long, type: KClass<out Any>, data: Map<KProperty<Any>, Any?>) {
    val sEntity = schema.find(type)
    Context.update(id, sEntity, data)
  }
}