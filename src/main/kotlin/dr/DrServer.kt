package dr

import dr.engine.ModificationEngine
import dr.engine.QueryEngine
import dr.io.DEntityTranslator
import dr.io.InputProcessor
import dr.schema.Schema
import dr.spi.IAuthorizer
import dr.spi.IModificationAdaptor
import dr.spi.IQueryAdaptor
import dr.spi.IQueryExecutor
import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.*
import io.javalin.http.Context
import java.util.*

class DrServer(
  val schema: Schema,
  private val authorizer: IAuthorizer,
  private val mAdaptor: IModificationAdaptor,
  private val qAdaptor: IQueryAdaptor
) {
  private val processor = InputProcessor(schema)
  private val translator = DEntityTranslator(schema)

  val qEngine = QueryEngine(schema, qAdaptor, authorizer)
  val mEngine = ModificationEngine(processor, translator, mAdaptor, authorizer)
  //val aEngine: ActionEngine
  //val nEngine: NotificationEngine

  var enabled = false
    private set

  fun start(port: Int) {
    val app = Javalin.create().start(port)

    app.get("/") { ctx -> ctx.result("Hello World") }

    app.before("/api/*") { ctx ->
      // TODO: authenticate
      println("before - /api/*")
    }

    app.routes {
      path("api") {
        get(this::getSchema)

        post("create/:entity", this::create)
        post("update/:entity/:id", this::update)

        path("query") {
          post("compile", this::compile)
        }
      }
    }

    enabled = true
  }

  private val queries = mutableMapOf<String, IQueryExecutor>()

  fun getSchema(ctx: Context) {
    println("getSchema")
  }

  fun create(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")

      val sEntity = schema.masters[entity] ?: throw Exception("Master entity not found! - ($entity)")
      val id = mEngine.create(sEntity, ctx.body())

      mapOf("@type" to "ok", "id" to id)
    } catch (ex: Exception) {
      mapOf("@type" to "error", "msg" to ex.message)
    }

    ctx.json(res)
  }

  fun update(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")
      val id = ctx.pathParam("id").toLong()

      val sEntity = schema.entities[entity] ?: throw Exception("Entity not found! - ($entity)")
      mEngine.update(sEntity, id, ctx.body())

      mapOf("@type" to "ok")
    } catch (ex: Exception) {
      mapOf("@type" to "error", "msg" to ex.message)
    }

    ctx.json(res)
  }

  fun compile(ctx: Context) {
    val res = try {
      val qExec = qEngine.compile(ctx.body())
      val uuid = UUID.randomUUID().toString()
      queries[uuid] = qExec

      mapOf("@type" to "ok", "uuid" to uuid)
    } catch (ex: Exception) {
      mapOf("@type" to "error", "msg" to ex.message)
    }

    ctx.json(res)
  }
}