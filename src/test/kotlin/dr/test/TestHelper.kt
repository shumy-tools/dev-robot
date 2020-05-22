package dr.test

import dr.ctx.Context
import dr.ctx.Session
import dr.io.InputProcessor
import dr.io.InputService
import dr.io.InstructionBuilder
import dr.io.Instructions
import dr.query.QueryService
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

class TestServer(private val schema: Schema, adaptor: IAdaptor) {
  private val machines = linkedMapOf<SEntity, Machine<*, *, *>>()
  private val processor = InputProcessor(schema)
  private val translator = InstructionBuilder(adaptor.tables)

  private val iService = InputService(processor, translator, adaptor, machines)
  private val qService = QueryService(adaptor.tables, adaptor)

  init {
    println("----Checking State Machines----")
    Context.session = Session(iService, qService)
      schema.masters.filter { it.value.machine != null }.forEach {
        machines[it.value] = buildMachine(it.value)
        println("    ${it.value.machine!!.name} - OK")
      }
    Context.clear()
  }

  fun create(type: KClass<out Any>, json: String): Instructions {
    Context.session = Session(iService, qService)
    return iService.initCreate(type, json)
  }

  fun update(type: KClass<out Any>, id: Long, json: String): Instructions {
    Context.session = Session(iService, qService)
    return iService.initUpdate(type, id, json)
  }

  fun query(query: String): List<QRow> {
    Context.session = Session(iService, qService)
    return Context.query(query).exec().rows
  }

  private fun buildMachine(sEntity: SEntity): Machine<*, *, *> {
    val instance = sEntity.machine!!.clazz.createInstance()
    instance.init(schema, sEntity, qService)
    return instance
  }
}