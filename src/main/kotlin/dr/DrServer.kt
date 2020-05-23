package dr

import dr.base.loadRoles
import dr.ctx.Session
import dr.io.InputProcessor
import dr.io.InputService
import dr.io.InstructionBuilder
import dr.query.QueryService
import dr.schema.RefID
import dr.schema.SEntity
import dr.schema.Schema
import dr.spi.IAdaptor
import dr.spi.IAuthorizer
import dr.spi.IQueryExecutor
import dr.spi.IReadAccess
import dr.state.Machine
import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.*
import io.javalin.http.Context
import java.security.MessageDigest
import java.util.*
import kotlin.reflect.full.createInstance

class DrServer(val schema: Schema, val adaptor: IAdaptor, val authorizer: IAuthorizer? = null) {
  private val queries = mutableMapOf<String, Pair<IQueryExecutor<Any>, IReadAccess>>()
  private val machines = linkedMapOf<SEntity, Machine<*, *, *>>()

  private val processor = InputProcessor(schema)
  private val translator = InstructionBuilder(adaptor.tables)

  private val iService = InputService(processor, translator, adaptor, machines)
  private val qService = QueryService(adaptor.tables, adaptor)

  init {
    println("----Checking State Machines----")
    dr.ctx.Context.session = Session(iService, qService)
      schema.masters.filter { it.value.machine != null }.forEach {
        machines[it.value] = buildMachine(it.value)
        println("    ${it.value.machine!!.name} - OK")
      }

      loadRoles()
    dr.ctx.Context.clear()
  }

  fun start(port: Int) {
    val app = Javalin.create().start(port)

    app.get("/") { ctx -> ctx.result("Hello World") }

    app.before("/api/*") { ctx ->
      // TODO: authenticate and set the correct user
      val user = dr.base.User("admin", "admin@mail.com", listOf(RefID(1)))

      println("before - /api/*")
      dr.ctx.Context.session = Session(iService, qService, user)
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
    dr.ctx.Context.session = Session(iService, qService)
      dr.ctx.Context.useFn()
      if (dr.ctx.Context.instructions.all.isNotEmpty())
        adaptor.commit(dr.ctx.Context.instructions)
    dr.ctx.Context.clear()
  }

  private fun buildMachine(sEntity: SEntity): Machine<*, *, *> {
    val instance = sEntity.machine!!.clazz.createInstance()
    instance.init(schema, sEntity, qService)
    return instance
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

      val main = iService.initCreate(entity, ctx.body())

      mapOf("@type" to "ok").plus(main.output)
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

      val main = iService.initUpdate(entity, id, ctx.body())

      mapOf("@type" to "ok").plus(main.output)
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

      iService.initAction(entity, evtType, id, ctx.body())

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