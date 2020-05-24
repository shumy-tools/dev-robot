package dr.state

import dr.JsonParser
import dr.base.*
import dr.ctx.Context
import dr.io.DEntity
import dr.schema.*
import dr.spi.IQueryExecutor
import dr.spi.QRow
import java.time.LocalDateTime
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1

const val NEW_HISTORY = "NEW_$HISTORY"

enum class PropertyState { OPEN, CLOSE }

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

  @Suppress("UNCHECKED_CAST")
  val state: S
    get() = sMachine.states.getValue(Context.session.vars[STATE] as String) as S

  val open: JMap
    get() = Context.session.vars[OPEN] as JMap

  val user: User
    get() = Context.session.user

  protected var onCreate: ((T) -> Unit)? = null
  protected var onUpdate: ((UEntity<T>) -> Unit)? = null

  private val enter = hashMapOf<String, EnterActions.() -> Unit>()
  private val events = hashMapOf<KClass<out E>, MutableMap<String, After<out E>>>()

  private lateinit var schema: Schema
  internal lateinit var sMachine: SMachine

  private lateinit var stateAndOpenQuery: IQueryExecutor<T>
  private lateinit var historyQuery: IQueryExecutor<T>

  internal fun init(allSchema: Schema, sEntity: SEntity) {
    schema = allSchema
    sMachine = sEntity.machine!!

    @Suppress("UNCHECKED_CAST")
    stateAndOpenQuery = query(sEntity.clazz,"| $ID == ?id | { $STATE, $OPEN }") as IQueryExecutor<T>

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
    val event = inEvt as E
    val evtType = event.javaClass.kotlin
    val so = stateAndOpenQuery.exec("id" to id)
    val state = so.get<String>(STATE)!!
    val open = so.get<JMap>(OPEN)!!

    Context.session.vars[ID] = RefID(id)
    Context.session.vars[STATE] = state
    Context.session.vars[OPEN] = open

    val states = events.getValue(evtType)
    val then = states[state] ?: throw Exception("StateMachine transition '$state -> ${evtType.qualifiedName}' not found! - (${javaClass.canonicalName})")

    val evtJson = JsonParser.write(inEvt)
    val newHistory = History(LocalDateTime.now(), user.name, evtType.qualifiedName, evtJson, state, then.to.name, JMap())
    Context.session.vars[NEW_HISTORY] = newHistory

    then.call(user, event)
    finalizeEvent(then.to.name)
  }

  @Suppress("UNCHECKED_CAST")
  internal fun fireCreate(entity: DEntity) {
    Context.session.vars[ID] = entity.refID
    Context.session.vars[STATE] = entity.dState
    Context.session.vars[OPEN] = entity.dOpen

    onCreate?.invoke(entity.cEntity as T)
    finalizeEvent(entity.dState)
  }

  internal fun fireUpdate(entity: DEntity) {
    val so = stateAndOpenQuery.exec("id" to entity.refID.id!!)

    Context.session.vars[ID] = entity.refID
    Context.session.vars[STATE] = so.get<String>(STATE)!!
    Context.session.vars[OPEN] = so.get<JMap>(OPEN)!!

    onUpdate?.invoke(UEntity(entity.mEntity!!))
  }

  @Suppress("UNCHECKED_CAST")
  private fun finalizeEvent(toState: String) {
    // call enter state if exists
    val enter = enter[toState]
    enter?.let {
      Context.session.vars[STATE] = toState
      it.invoke(EnterActions())
    }
  }

  inner class SHistory internal constructor() {
    inner class Record<EX: E>(private val map: QRow) {
      val ts: LocalDateTime by lazy {
        map.getValue(History::ts.name) as LocalDateTime
      }

      val user: String by lazy {
        map.getValue(History::user.name) as String
      }

      @Suppress("UNCHECKED_CAST")
      val evtType: KClass<EX>? by lazy {
        val value = map[History::evtType.name] as String?
        value?.let { sMachine.events[value] as KClass<EX>? }
      }

      val event: EX? by lazy {
        val type = evtType
        type?.let {
          val value = map.getValue(History::evt.name) as String
          JsonParser.read(value, type)
        }
      }

      @Suppress("UNCHECKED_CAST")
      val from: S? by lazy {
        val value = map.getValue(History::from.name) as String
        sMachine.states.getValue(value) as S
      }

      @Suppress("UNCHECKED_CAST")
      val to: S by lazy {
        val value = map.getValue(History::to.name) as String
        sMachine.states.getValue(value) as S
      }

      val data: JMap by lazy {
        map.getValue(History::data.name) as JMap
      }

      operator fun get(key: String) = data[key]!!

      override fun toString() = "Record(ts=$ts, user=$user, event=$event, from=$from, to=$to, data=$data)"
    }

    private val newData by lazy {
      val newHistory = Context.session.vars[NEW_HISTORY] as History
      newHistory.data
    }

    val all: List<Machine<T, S, E>.SHistory.Record<E>>
    init {
      val id = (Context.session.vars[ID] as RefID).id

      @Suppress("UNCHECKED_CAST")
      val history = (if (id != null) historyQuery.exec("id" to id).get<Any>(HISTORY)!! else emptyList<Any>()) as List<QRow>
      all = history.map { Record<E>(it) }
    }

    operator fun set(key: String, value: Any) {
      newData[key] = value
    }

    @Suppress("UNCHECKED_CAST")
    fun <EX: E> last(evtType: KClass<EX>, from: S? = null, to: S? = null): Record<EX> {
      return all.last { it.evtType == evtType && (from == null || it.from == from) && (to == null || it.to == to) } as Record<EX>
    }
  }

  inner class For internal constructor(private val pState: PropertyState, private val prop: KProperty<*>) {
    fun forAny() {
      val oField = open.getOrPut(prop.name)
      when (pState) {
        PropertyState.OPEN -> oField[ANY] = true
        PropertyState.CLOSE -> open[prop.name] = null
      }
    }

    fun forRole(role: String) {
      val oField = open.getOrPut(prop.name)
      val oRoles = oField.getOrPut(ROLES)
      when (pState) {
        PropertyState.OPEN -> if (oField[ANY] == null) oRoles[role] = true
        PropertyState.CLOSE -> {
          oRoles[role] = null
          if (oRoles.isEmpty()) {
            oField[ROLES] = null
            if (oField[USERS] == null) open[prop.name] = null
          }
        }
      }
    }

    fun forUser(user: String) {
      val oField = open.getOrPut(prop.name)
      val oUsers = oField.getOrPut(USERS)
      when (pState) {
        PropertyState.OPEN -> if (oField[ANY] == null) oUsers[user] = true
        PropertyState.CLOSE -> {
          oUsers[user] = null
          if (oUsers.isEmpty()) {
            oField[USERS] = null
            if (oField[ROLES] == null) open[prop.name] = null
          }
        }
      }
    }
  }

  open inner class EnterActions internal constructor() {
    val history by lazy { SHistory() }

    fun open(prop: KProperty1<T, Any>): For {
      return For(PropertyState.OPEN, prop)
    }

    fun close(prop: KProperty1<T, Any>): For {
      return For(PropertyState.CLOSE, prop)
    }

    internal var checkOrder = 0
    open infix fun check(predicate: () -> Boolean) {
      checkOrder++
      if (!predicate())
        throw Exception("State machine failed on check constraint! - (enter $state, check $checkOrder for ${this@Machine.javaClass.canonicalName})")
    }
  }

  inner class EventActions<EX: E> internal constructor(val event: EX): EnterActions() {
    override infix fun check(predicate: () -> Boolean) {
      checkOrder++
      if (!predicate())
        throw Exception("State machine failed on check constraint! - ($state -> ${event.javaClass.simpleName}, check $checkOrder for ${this@Machine.javaClass.canonicalName})")
    }
  }

  inner class After<EX: E> internal constructor(internal val to: S, private val users: Set<String>, private val roles: Set<String>) {
    private var onEvent: (EventActions<out E>.() -> Unit)? = null

    @Suppress("UNCHECKED_CAST")
    infix fun after(exec: EventActions<EX>.() -> Unit) { onEvent = exec as EventActions<out E>.() -> Unit }

    fun call(user: User, event: E) {
      val rolesMap = user.rolesMap()
      if (roles.contains(ANY) || users.contains(user.name) || roles.any(rolesMap.keys::contains)) {
        onEvent?.invoke(EventActions(event))
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
      val then = After<EX>(to, emptySet(), setOf(ANY))
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