package dr.state

import dr.JsonParser
import dr.base.History
import dr.base.User
import dr.ctx.Context
import dr.io.DEntity
import dr.query.QueryService
import dr.schema.RefID
import dr.schema.SEntity
import dr.schema.SMachine
import dr.schema.Schema
import dr.schema.tabular.HISTORY
import dr.schema.tabular.ID
import dr.schema.tabular.STATE
import dr.spi.IQueryExecutor
import dr.spi.QRow
import java.time.LocalDateTime
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1

const val NEW_HISTORY = "NEW_$HISTORY"
private const val NEW_HISTORY_DATA = "${NEW_HISTORY}_DATA"

class UEntity<T: Any>(val data: Map<String, Any?>) {
  @Suppress("UNCHECKED_CAST")
  fun <R: Any?> get(prop: KProperty1<T, R>): R {
    return data[prop.name] as R
  }

  override fun toString(): String {
    return JsonParser.write(data)
  }
}

open class Machine<T: Any, S: Enum<*>, E: Any> {
  val id: RefID
    get() = Context.session.vars[ID] as RefID

  val user: User
    get() = Context.session.user

  var onCreate: ((T) -> Unit)? = null
  var onUpdate: ((UEntity<T>) -> Unit)? = null

  private val enter = hashMapOf<String, EnterActions.() -> Unit>()
  private val events = hashMapOf<KClass<out E>, MutableMap<String, After<out E>>>()

  private lateinit var schema: Schema
  internal lateinit var sMachine: SMachine

  private lateinit var stateQuery: IQueryExecutor<T>
  private lateinit var historyQuery: IQueryExecutor<T>

  internal fun init(allSchema: Schema, sEntity: SEntity, qService: QueryService) {
    schema = allSchema
    sMachine = sEntity.machine!!

    @Suppress("UNCHECKED_CAST")
    stateQuery = query(sEntity.clazz,"| $ID == ?id | { $STATE }") as IQueryExecutor<T>

    @Suppress("UNCHECKED_CAST")
    historyQuery = query(sEntity.clazz,"| $ID == ?id | { $ID, $HISTORY { * } }") as IQueryExecutor<T>
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
    Context.session.vars[ID] = RefID(id)
    val event = inEvt as E
    val evtType = event.javaClass.kotlin

    val state = stateQuery.exec("id" to id).get<String>(STATE)!!
    Context.session.vars[STATE] = state

    val states = events.getValue(evtType)
    val then = states[state] ?: throw Exception("StateMachine transition '$state -> ${evtType.qualifiedName}' not found! - (${javaClass.canonicalName})")

    val evtJson = JsonParser.write(inEvt)
    val newHistory = History(LocalDateTime.now(), evtType.qualifiedName, evtJson, state, then.to.name)
    Context.session.vars[NEW_HISTORY] = newHistory

    then.call(user, state, event)
    finalizeEvent(then.to.name)
  }

  @Suppress("UNCHECKED_CAST")
  internal fun fireCreate(entity: DEntity) {
    Context.session.vars[ID] = entity.refID
    onCreate?.invoke(entity.cEntity as T)
    finalizeEvent(entity.state)
  }

  internal fun fireUpdate(entity: DEntity) {
    Context.session.vars[ID] = entity.refID
    onUpdate?.invoke(UEntity(entity.mEntity!!))
  }

  @Suppress("UNCHECKED_CAST")
  private fun finalizeEvent(toState: String) {
    // call enter state if exists
    val enter = enter[toState]
    enter?.invoke(EnterActions(toState))

    // finalize the data result
    val newData = Context.session.vars[NEW_HISTORY_DATA]
    newData?.let {
      val newHistory = Context.session.vars[NEW_HISTORY] as History
      newHistory.data = JsonParser.write(newData)
    }
  }

  inner class SHistory internal constructor(private val state: String, private val evtType: KClass<out E>? = null) {
    inner class Record<EX: E>(private val map: QRow) {
      val ts: LocalDateTime by lazy {
        map.getValue(History::ts.name) as LocalDateTime
      }

      @Suppress("UNCHECKED_CAST")
      val evtType: KClass<EX>? by lazy {
        val value = map[History::evtType.name] as String?
        value?.let { sMachine.events[value] as KClass<EX>? }
      }

      @Suppress("UNCHECKED_CAST")
      val event: EX? by lazy {
        val type = evtType
        type?.let {
          val value = map.getValue(History::evt.name) as String
          JsonParser.read(value, type) as EX
        }
      }

      @Suppress("UNCHECKED_CAST")
      val from: S? by lazy {
        val value = map.getValue(History::from.name) as String?
        value?.let { sMachine.states.getValue(value) as S }
      }

      @Suppress("UNCHECKED_CAST")
      val to: S by lazy {
        val value = map.getValue(History::to.name) as String
        sMachine.states.getValue(value) as S
      }

      val data: Map<String, Any> by lazy {
        val value = map.getValue(History::data.name) as String
        JsonParser.readMap(value)
      }

      override fun toString() = "Record(ts=$ts, event=$event, from=$from, to=$to, data=$data)"
    }

    private val newData by lazy {
      val newHistory = Context.session.vars[NEW_HISTORY] as History
      val map = JsonParser.readMap(newHistory.data).toMutableMap()
      Context.session.vars[NEW_HISTORY_DATA] = map
      map
    }

    val all: List<Machine<T, S, E>.SHistory.Record<E>>
    init {
      val id = (Context.session.vars[ID] as RefID).id

      @Suppress("UNCHECKED_CAST")
      val history = (if (id != null) historyQuery.exec("id" to id).get<Any>(HISTORY)!! else emptyList<Any>()) as List<QRow>
      all = history.map { Record<E>(it) }
    }

    fun set(key: String, value: Any) {
      newData[key] = value
    }

    @Suppress("UNCHECKED_CAST")
    fun <EX: E> last(evtType: KClass<EX>, from: S? = null, to: S? = null): Record<EX> {
      return all.last { it.evtType == evtType && (from == null || it.from == from) && (to == null || it.to == to) } as Record<EX>
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
    val history by lazy { SHistory(state, evtType) }

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
        throw Exception("State machine failed on check constraint! - (enter $state, check $checkOrder for ${this@Machine.javaClass.canonicalName})")
    }
  }

  inner class EventActions<EX: E> internal constructor(private val state: String, val event: EX): EnterActions(state) {
    override infix fun check(predicate: () -> Boolean) {
      checkOrder++
      if (!predicate())
        throw Exception("State machine failed on check constraint! - ($state -> ${event.javaClass.simpleName}, check $checkOrder for ${this@Machine.javaClass.canonicalName})")
    }
  }

  inner class After<EX: E> internal constructor(internal val to: S, private val users: Set<String>, private val roles: Set<String>) {
    private var onEvent: (EventActions<out E>.() -> Unit)? = null
      private set

    @Suppress("UNCHECKED_CAST")
    infix fun after(exec: EventActions<EX>.() -> Unit) { onEvent = exec as EventActions<out E>.() -> Unit }

    fun call(user: User, state: String, event: E) {
      val rolesMap = user.rolesMap()
      if (users.contains(user.name) || roles.any(rolesMap.keys::contains)) {
        onEvent?.invoke(EventActions(state, event))
      }
    }
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

  fun <T: Any> query(type: KClass<T>, query: String) = Context.query(type, query)

  fun create(entity: Any) = Context.create(entity)

  fun update(id: Long, type: KClass<out Any>, data: Map<KProperty<Any>, Any?>) {
    val sEntity = schema.find(type)
    Context.update(id, sEntity, data)
  }

  fun action(id: Long, type: KClass<out Any>, evt: Any) {
    val sEntity = schema.find(type)
    Context.action(id, sEntity, evt)
  }
}