package dr.io

import dr.JsonParser
import dr.base.History
import dr.ctx.Context
import dr.schema.Pack
import dr.schema.RefID
import dr.schema.SEntity
import dr.schema.tabular.HISTORY
import dr.schema.tabular.STATE
import dr.spi.IAdaptor
import dr.state.Machine
import dr.state.NEW_HISTORY
import java.time.LocalDateTime
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

class InputService(
  private val processor: InputProcessor,
  private val translator: InstructionBuilder,
  private val adaptor: IAdaptor,
  private val machines: Map<SEntity, Machine<*, *, *>>
) {
  private val schema = processor.schema

  fun create(data: Any): RefID {
    val dEntity = processor.create(data)
    val dHistory = dEntity.createStateHistory()

    val main = translator.create(dEntity)
    val root = if (dHistory == null) main else {
      val hInst = translator.create(dHistory)
      hInst.include(main)
      hInst
    }

    Context.instructions.include(root)
    return root.root.refID
  }

  fun update(id: Long, type: SEntity, data: Map<KProperty<Any>, Any?>) {
    val nData = data.mapKeys { it.key.name }
    val dEntity = processor.update(type, id, nData)
    machines[dEntity.schema]?.fireUpdate(dEntity)

    val main = translator.update(dEntity)
    Context.instructions.include(main)
  }

  fun action(id: Long, type: SEntity, evt: Any) {
    val machine = machines[type] ?: throw Exception("Entity doesn't have a state machine! - (${type.name})")
    val evtType = evt.javaClass.canonicalName

    if (!machine.sMachine.events.containsKey(evtType))
      throw Exception("Event type not found! - ($type, $evtType)")

    machine.fireEvent(id, evt)
  }

  fun initCreate(type: KClass<out Any>, json: String) = initCreate(type.qualifiedName!!, json)

  fun initCreate(entity: String, json: String): Instructions {
    val dEntity = if (entity == Pack::class.qualifiedName) {
      processor.create(Pack::class, json)
    } else {
      val sEntity = schema.masters[entity] ?: throw Exception("Master entity not found! - ($entity)")
      processor.create(sEntity, json)
    }

    val dHistory = dEntity.createStateHistory()

    val main = translator.create(dEntity)
    val root = if (dHistory == null) main else {
      val hInst = translator.create(dHistory)
      hInst.include(main)
      hInst.root = main.root
      hInst
    }

    root.include(Context.instructions)
    adaptor.commit(root)
    return root
  }

  fun initUpdate(type: KClass<out Any>, id: Long, json: String) = initUpdate(type.qualifiedName!!, id, json)

  fun initUpdate(entity: String, id: Long, json: String): Instructions {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity not found! - ($entity)")
    val dEntity = processor.update(sEntity, id, json)
    machines[dEntity.schema]?.fireUpdate(dEntity)

    val root = translator.update(dEntity)

    root.include(Context.instructions)
    adaptor.commit(root)
    return root
  }

  fun initAction(type: KClass<out Any>, evtType: KClass<out Any>, id: Long, json: String) = initAction(type.qualifiedName!!, evtType.qualifiedName!!, id, json)

  fun initAction(entity: String, evtType: String, id: Long, json: String): Instructions {
    val sEntity = schema.entities[entity] ?: throw Exception("Entity not found! - ($entity)")
    val machine = machines[sEntity] ?: throw Exception("Entity doesn't have a state machine! - ($entity)")
    val evtClass = machine.sMachine.events[evtType] ?: throw Exception("Event type not found! - ($entity, $evtType)")

    val event = JsonParser.readJson(json, evtClass)
    machine.fireEvent(id, event)

    // create and link NEW_HISTORY
    val history = Context.session.vars[NEW_HISTORY] as History
    val refID = create(history)
    val uData = mapOf(STATE to history.to, HISTORY to OneLinkWithoutTraits(refID))
    updateState(id, sEntity, uData)

    adaptor.commit(Context.instructions)
    return Context.instructions
  }

  private fun updateState(id: Long, type: SEntity, data: Map<String, Any?>) {
    val dEntity = processor.update(type, id, data, true)
    val main = translator.update(dEntity)
    Context.instructions.include(main)
  }

  private fun DEntity.createStateHistory(): DEntity? {
    // add history if state-machine exists
    val sMachine = schema.machine
    return sMachine?.let {
      val state = sMachine.states.keys.first()
      val history = History(LocalDateTime.now(), null,null, null, state)
      Context.session.vars[NEW_HISTORY] = history

      val dHistory = processor.create(history)
      setHistory(dHistory)
      machines[schema]?.fireCreate(this)

      dHistory
    }
  }
}