package dr.test

import dr.base.User
import dr.base.loadRoles
import dr.ctx.Context
import dr.ctx.Session
import dr.io.InputProcessor
import dr.io.InputService
import dr.io.InstructionBuilder
import dr.io.Instructions
import dr.query.QueryService
import dr.schema.RefID
import dr.schema.SEntity
import dr.schema.Schema
import dr.spi.IAdaptor
import dr.spi.IAuthorizer
import dr.spi.IReadAccess
import dr.spi.QRow
import dr.state.Machine
import kotlin.reflect.KClass
import kotlin.reflect.full.createInstance

class TestAuthorizer: IAuthorizer {
  override fun read(access: IReadAccess) = true
}

class TestServer(private val schema: Schema, private val adaptor: IAdaptor) {
  private val machines = linkedMapOf<SEntity, Machine<*, *, *>>()
  private val processor = InputProcessor(schema)
  private val translator = InstructionBuilder(adaptor.tables)

  private val iService = InputService(processor, translator, adaptor, machines)
  private val qService = QueryService(adaptor.tables, adaptor)

  val adminUser = User("shumy", "shumy@gmail.com", listOf(RefID(1)))
  val managerUser = User("alex", "alex@gmail.com", listOf(RefID(2)))

  init {
    println("----Checking State Machines----")
    Context.session = Session(adaptor.tables, iService, qService)
      schema.masters.filter { it.value.machine != null }.forEach {
        machines[it.value] = buildMachine(it.value)
        println("    ${it.value.machine!!.name} - OK")
      }

    loadRoles()
    Context.clear()
  }

  fun create(entType: KClass<out Any>, json: String, user: User = adminUser): Instructions {
    Context.session = Session(adaptor.tables, iService, qService, user)
    return iService.initCreate(entType, json)
  }

  fun update(entType: KClass<out Any>, id: Long, json: String, user: User = adminUser): Instructions {
    Context.session = Session(adaptor.tables, iService, qService, user)
    return iService.initUpdate(entType, id, json)
  }

  fun action(entType: KClass<out Any>, evtType: KClass<out Any>, id: Long, json: String, user: User = adminUser): Instructions {
    Context.session = Session(adaptor.tables, iService, qService, user)
    return iService.initAction(entType, evtType, id, json)
  }

  fun <T: Any> query(type: KClass<T>, query: String, user: User = adminUser): List<QRow> {
    Context.session = Session(adaptor.tables, iService, qService, user)
    return Context.query(type, query).exec().rows
  }

  private fun buildMachine(sEntity: SEntity): Machine<*, *, *> {
    val instance = sEntity.machine!!.clazz.createInstance()
    instance.init(schema, sEntity)
    return instance
  }
}