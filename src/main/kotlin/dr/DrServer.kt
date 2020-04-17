package dr

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dr.engine.ModificationEngine
import dr.engine.QueryEngine
import dr.io.*
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
  private val mapper: ObjectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)

  private val processor = InputProcessor(schema, mapper)
  private val translator = DEntityTranslator(schema)

  val qEngine = QueryEngine(schema, qAdaptor, authorizer)
  val mEngine = ModificationEngine(processor, translator, mAdaptor, authorizer)
  //val aEngine: ActionEngine
  //val nEngine: NotificationEngine

  var enabled = false
    private set

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
    val isSimple = ctx.queryParam("simple") != null
    val res = schema.toMap(isSimple)

    val json = mapper.writeValueAsString(mapOf<String, Any>("@type" to "ok").plus(res))
    ctx.result(json).contentType("application/json")
  }

  fun create(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")

      val sEntity = schema.masters[entity] ?: throw Exception("Master entity not found! - ($entity)")
      val output = mEngine.create(sEntity, ctx.body())

      mapOf("@type" to "ok").plus(output)
    } catch (ex: Exception) {
      mapOf("@type" to "error", "msg" to ex.message)
    }

    val json = mapper.writeValueAsString(res)
    ctx.result(json).contentType("application/json")
  }

  fun update(ctx: Context) {
    val res = try {
      val entity = ctx.pathParam("entity")
      val id = ctx.pathParam("id").toLong()

      val sEntity = schema.entities[entity] ?: throw Exception("Entity not found! - ($entity)")
      val output = mEngine.update(sEntity, id, ctx.body())

      mapOf("@type" to "ok").plus(output)
    } catch (ex: Exception) {
      mapOf("@type" to "error", "msg" to ex.message)
    }

    val json = mapper.writeValueAsString(res)
    ctx.result(json).contentType("application/json")
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

    val json = mapper.writeValueAsString(res)
    ctx.result(json).contentType("application/json")
  }
}