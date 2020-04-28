package dr

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dr.ctx.Session
import dr.io.*
import dr.query.QueryService
import dr.schema.SEntity
import dr.schema.Schema
import dr.spi.IAdaptor
import dr.spi.IAuthorizer
import dr.spi.IQueryExecutor
import dr.spi.IReadAccess
import dr.state.Machine
import dr.state.buildMachine
import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.*
import io.javalin.http.Context
import java.security.MessageDigest
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

  @Suppress("UNCHECKED_CAST")
  fun readMap(json: String): Map<String, Any> {
    return mapper.readValue(json, Map::class.java) as Map<String, Any>
  }
}

class DrServer(val schema: Schema, val adaptor: IAdaptor, val authorizer: IAuthorizer? = null) {
  internal val processor = InputProcessor(schema)
  internal val translator = InstructionBuilder(adaptor.tables())
  internal val qService = QueryService(schema, adaptor)

  private val machines: Map<SEntity, Machine<*, *>>
  private val queries = mutableMapOf<String, Pair<IQueryExecutor, IReadAccess>>()

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

        post("query", this::query)
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
      adaptor.commit(instructions)

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
      adaptor.commit(instructions)

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

  fun query(ctx: Context) {
    val res = try {
      val isInline = ctx.queryParam("inline") != null
      val isCompile = ctx.queryParam("compile") != null
      val exec = ctx.queryParam("exec")

      when {
        isInline -> {
          val (query, access) = qService.compile(ctx.body())
          // TODO: check access control from query.accessed

          val res = query.exec()
          mutableMapOf<String, Any>("@type" to "ok").apply {
            this["data"] = res.raw()
          }
        }

        isCompile -> {
          val uuid = ctx.body().hash()
          if (queries[uuid] == null)
            queries[uuid] = qService.compile(ctx.body())

          mapOf("@type" to "ok", "uuid" to uuid)
        }

        exec != null -> {
          val (query, access) = queries[exec] ?: throw Exception("Compiled query not found! - ($exec)")
          // TODO: check access control from query.accessed

          val params = JsonParser.readMap(ctx.body())
          val res = query.exec(params)

          mutableMapOf<String, Any>("@type" to "ok").apply {
            this["data"] = res.raw()
          }
        }

        else -> throw Exception("Unrecognized query command!")
      }
    } catch (ex: Exception) {
      ex.printStackTrace()
      mapOf("@type" to "error", "msg" to ex.message)
    }

    val json = JsonParser.write(res)
    ctx.result(json).contentType("application/json")
  }
}

fun String.hash(): String {
  val compact = replace(Regex("\\s"), "")

  val digest = MessageDigest.getInstance("SHA-256")
  val bytes = digest.digest(compact.toByteArray())
  return Base64.getUrlEncoder().encodeToString(bytes)
}