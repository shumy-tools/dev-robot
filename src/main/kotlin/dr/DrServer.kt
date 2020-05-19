package dr

import dr.ctx.Session
import dr.io.InputProcessor
import dr.io.InstructionBuilder
import dr.io.Instructions
import dr.io.JsonParser
import dr.query.QueryService
import dr.schema.Pack
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

class DrServer(val schema: Schema, val adaptor: IAdaptor, val authorizer: IAuthorizer? = null) {
  internal val processor: InputProcessor
  internal val translator = InstructionBuilder(adaptor.tables)
  internal val qService = QueryService(adaptor.tables, adaptor)

  private val machines: Map<SEntity, Machine<*, *>>
  private val queries = mutableMapOf<String, Pair<IQueryExecutor, IReadAccess>>()

  init {
    println("----Checking State Machines----")
    dr.ctx.Context.session = Session(this)
      machines = schema.masters.filter { it.value.machine != null }.map {
        val instance = it.value to buildMachine(it.value)
        println("    ${it.value.machine!!.name} - OK")
        instance
      }.toMap()
    dr.ctx.Context.clear()
    processor = InputProcessor(schema, machines)
  }

  fun start(port: Int) {
    val app = Javalin.create().start(port)

    app.get("/") { ctx -> ctx.result("Hello World") }

    app.before("/api/*") { ctx ->
      // TODO: authenticate and set the correct user
      val user = dr.base.User("admin", "admin@mail.com", listOf(1L))

      println("before - /api/*")
      dr.ctx.Context.session = Session(this, user)
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

  fun use(useFn: dr.ctx.Context.() -> Unit) {
    val instructions = Instructions()
    dr.ctx.Context.session = Session(this)
    dr.ctx.Context.instructions = instructions
      dr.ctx.Context.useFn()
      if (instructions.all.isNotEmpty())
        adaptor.commit(instructions)
    dr.ctx.Context.clear()
  }

  private fun schema(ctx: Context) {
    val isSimple = ctx.queryParam("simple") != null
    val res = schema.toMap(isSimple)

    val json = JsonParser.write(mapOf<String, Any>("@type" to "ok").plus(res))
    ctx.result(json).contentType("application/json")
  }

  private fun check(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")

      val sEntity = schema.entities[entity] ?: throw Exception("Entity not found! - ($entity)")
      val dEntity = processor.update(sEntity, 0L, ctx.body())

      mapOf("@type" to "ok").plus(dEntity.checkFields())
    } catch (ex: Exception) {
      ex.handleError()
    }

    val json = JsonParser.write(res)
    ctx.result(json).contentType("application/json")
  }

  private fun create(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")

      // TODO: check access control

      val dEntity = if (entity == Pack::class.qualifiedName) {
        processor.create(Pack::class, ctx.body())
      } else {
        val sEntity = schema.masters[entity] ?: throw Exception("Master entity not found! - ($entity)")
        processor.create(sEntity, ctx.body())
      }

      val instructions = translator.create(dEntity)
      dr.ctx.Context.instructions = instructions
      adaptor.commit(instructions)

      mapOf("@type" to "ok").plus(instructions.output)
    } catch (ex: Exception) {
      ex.handleError()
    }

    val json = JsonParser.write(res)
    ctx.result(json).contentType("application/json")
  }

  private fun update(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")
      val id = ctx.pathParam("id").toLong()

      // TODO: check access control

      val sEntity = schema.entities[entity] ?: throw Exception("Entity not found! - ($entity)")
      val dEntity = processor.update(sEntity, id, ctx.body())

      val instructions = translator.update(id, dEntity)
      dr.ctx.Context.instructions = instructions
      adaptor.commit(instructions)

      mapOf("@type" to "ok").plus(instructions.output)
    } catch (ex: Exception) {
      ex.handleError()
    }

    val json = JsonParser.write(res)
    ctx.result(json).contentType("application/json")
  }

  private fun action(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")
      val id = ctx.pathParam("id").toLong()
      val evtType = ctx.queryParam("evt") ?: throw Exception("Event type is not specified for state machine! - ($entity)")

      // TODO: check access control

      val sEntity = schema.entities[entity] ?: throw Exception("Entity not found! - ($entity)")
      val machine = machines[sEntity] ?: throw Exception("Entity doesn't have a state machine! - ($entity)")
      val evtClass = machine.sMachine.events[evtType] ?: throw Exception("Event ype not found! - ($entity, $evtType)")

      val event = JsonParser.readJson(ctx.body(), evtClass)
      machine.onEvent(id, event)

      mapOf("@type" to "ok")
    } catch (ex: Exception) {
      ex.handleError()
    }

    val json = JsonParser.write(res)
    ctx.result(json).contentType("application/json")
  }

  private fun query(ctx: Context) {
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
            val data = res.rows
            this["count"] = data.size
            this["data"] = data
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
            this["data"] = res.rows
          }
        }

        else -> throw Exception("Unrecognized query command!")
      }
    } catch (ex: Exception) {
      ex.handleError()
    }

    val json = JsonParser.write(res)
    ctx.result(json).contentType("application/json")
  }
}

private fun Exception.handleError(): Map<String, Any> {
  printStackTrace()
  val message = (if (cause != null) cause!!.message else message) ?: "Unrecognized error!"
  return mapOf("@type" to "error", "msg" to message.replace('"', '\''))
}

private fun String.hash(): String {
  val compact = replace(Regex("\\s"), "")

  val digest = MessageDigest.getInstance("SHA-256")
  val bytes = digest.digest(compact.toByteArray())
  return Base64.getUrlEncoder().encodeToString(bytes)
}