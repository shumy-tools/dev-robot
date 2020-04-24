package dr

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dr.ctx.Session
import dr.query.QueryService
import dr.io.*
import dr.schema.SEntity
import dr.schema.Schema
import dr.spi.IAuthorizer
import dr.spi.IModificationAdaptor
import dr.spi.IQueryAdaptor
import dr.spi.IQueryExecutor
import dr.state.Machine
import dr.state.buildMachine
import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.*
import io.javalin.http.Context
import java.util.*
import kotlin.reflect.KClass

object JsonParser {
  val mapper: ObjectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)

  init {
    mapper.registerSubtypes(
      ManyLinksWithoutTraits::class.java,
      OneLinkWithoutTraits::class.java,
      ManyLinksWithTraits::class.java,
      OneLinkWithTraits::class.java,
      ManyUnlink::class.java,
      OneUnlink::class.java
    )
  }

  fun write(value: Any): String {
    return mapper.writeValueAsString(value)
  }

  fun <T: Any> read(json: String, type: KClass<out T>): T {
    return mapper.readValue(json, type.java)
  }
}

class DrServer(
  val schema: Schema,
  val authorizer: IAuthorizer,
  val mAdaptor: IModificationAdaptor,
  val qAdaptor: IQueryAdaptor
) {
  internal val processor = InputProcessor(schema)
  internal val translator = DEntityTranslator(schema)
  internal val qService = QueryService(schema, qAdaptor)

  private val machines: Map<SEntity, Machine<*, *>>
  private val queries = mutableMapOf<String, IQueryExecutor>()

  init {
    println("----Checking State Machines----")
    dr.ctx.Context.set(Session(this))
      machines = schema.masters.filter { it.value.machine != null }.map {
        val instance = it.value to buildMachine(it.value)
        println("    ${it.value.machine!!.name} - OK")
        instance
      }.toMap()
    dr.ctx.Context.clear()
  }

  fun start(port: Int) {
    val app = Javalin.create().start(port)

    app.get("/") { ctx -> ctx.result("Hello World") }

    app.before("/api/*") { ctx ->
      // TODO: authenticate and set the correct user
      val user = dr.base.User("admin", "admin@mail.com", listOf(1L))

      println("before - /api/*")
      val session = Session(this, user)
      dr.ctx.Context.set(session)
    }

    app.before("/api/*") { dr.ctx.Context.clear() }

    app.routes {
      path("api") {
        get(this::schema)

        post("check/:entity", this::check)

        post("create/:entity", this::create)
        post("update/:entity/:id", this::update)
        post("action/:entity/:id", this::action)


        path("query") {
          post("compile", this::compile)
        }
      }
    }
  }

  fun schema(ctx: Context) {
    val isSimple = ctx.queryParam("simple") != null
    val res = schema.toMap(isSimple)

    val json = JsonParser.write(mapOf<String, Any>("@type" to "ok").plus(res))
    ctx.result(json).contentType("application/json")
  }

  fun check(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")

      val sEntity = schema.entities[entity] ?: throw Exception("Entity not found! - ($entity)")
      val dEntity = processor.update(sEntity, ctx.body())

      mapOf("@type" to "ok").plus(dEntity.checkFields())
    } catch (ex: Exception) {
      mapOf("@type" to "error", "msg" to ex.message)
    }

    val json = JsonParser.write(res)
    ctx.result(json).contentType("application/json")
  }

  fun create(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")

      // TODO: check access control

      val sEntity = schema.masters[entity] ?: throw Exception("Master entity not found! - ($entity)")
      val dEntity = processor.create(sEntity, ctx.body())

      // execute machine if exists
      machines[sEntity]?.let { it.onCreate(dEntity) }

      val instructions = translator.create(dEntity)
      mAdaptor.commit(instructions)

      mapOf("@type" to "ok").plus(instructions.output)
    } catch (ex: Exception) {
      mapOf("@type" to "error", "msg" to ex.message)
    }

    val json = JsonParser.write(res)
    ctx.result(json).contentType("application/json")
  }

  fun update(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")
      val id = ctx.pathParam("id").toLong()

      // TODO: check access control

      val sEntity = schema.entities[entity] ?: throw Exception("Entity not found! - ($entity)")
      val dEntity = processor.update(sEntity, ctx.body())

      // execute machine if exists
      machines[sEntity]?.let { it.onUpdate(id, dEntity) }

      val instructions = translator.update(id, dEntity)
      mAdaptor.commit(instructions)

      mapOf("@type" to "ok").plus(instructions.output)
    } catch (ex: Exception) {
      mapOf("@type" to "error", "msg" to ex.message)
    }

    val json = JsonParser.write(res)
    ctx.result(json).contentType("application/json")
  }

  fun action(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")
      val id = ctx.pathParam("id").toLong()
      val evtType = ctx.queryParam("evt") ?: throw Exception("Event type is not specified for state machine! - ($entity)")

      // TODO: check access control

      val sEntity = schema.entities[entity] ?: throw Exception("Entity not found! - ($entity)")
      val machine = machines[sEntity] ?: throw Exception("Entity doesn't have a state machine! - ($entity)")
      val evtClass = machine.sMachine.events[evtType] ?: throw Exception("Event ype not found! - ($entity, $evtType)")

      val event = JsonParser.read(ctx.body(), evtClass)
      machine.onEvent(id, event)

      mapOf("@type" to "ok")
    } catch (ex: Exception) {
      mapOf("@type" to "error", "msg" to ex.message)
    }

    val json = JsonParser.write(res)
    ctx.result(json).contentType("application/json")
  }

  fun compile(ctx: Context) {
    val res = try {
      val qExec = qService.compile(ctx.body())

      // TODO: check access control from qExec.accessed

      val uuid = UUID.randomUUID().toString()
      queries[uuid] = qExec

      mapOf("@type" to "ok", "uuid" to uuid)
    } catch (ex: Exception) {
      mapOf("@type" to "error", "msg" to ex.message)
    }

    val json = JsonParser.write(res)
    ctx.result(json).contentType("application/json")
  }
}