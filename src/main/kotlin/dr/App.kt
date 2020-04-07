package dr

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dr.modification.ModificationEngine
import dr.query.QTree
import dr.query.QueryEngine
import dr.schema.*
import dr.spi.*
import java.time.LocalDateTime
import kotlin.random.Random

val mapper: ObjectWriter = jacksonObjectMapper()
  .registerModule(JavaTimeModule())
  .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
  .setSerializationInclusion(JsonInclude.Include.NON_NULL)
  .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
  .writerWithDefaultPrettyPrinter()

class TestResult(): IResult {
  override fun toJson(): String {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
  }
}

class TestQueryExecutor(): IQueryExecutor {
  override fun exec(params: Map<String, Any>): IResult {
    return TestResult()
  }
}

class TestQueryAdaptor: IQueryAdaptor {
  override fun compile(query: QTree): IQueryExecutor {
    val json = mapper.writeValueAsString(query)

    println("tree = $json")
    return TestQueryExecutor()
  }
}

class TestQueryAuthorizer : IQueryAuthorizer {
  override fun authorize(accessed: IAccessed): Boolean {
    println("accessed = $accessed")
    return true
  }
}

class TestModificationAdaptor: IModificationAdaptor {
  var idSeq = 9L;

  override fun commit(instructions: Instructions): Long {
    println("TX-START")
    val id = instructions.exec {
      when (it) {
        is Insert -> { println("  (${++idSeq}) -> $it"); idSeq }
        is Update -> { println("  (${it.id}) -> $it"); it.id }
        is Delete -> { println("  (${it.id}) -> $it"); it.id }
      }
    }
    println("TX-COMMIT")
    return id
  }
}

fun main(args: Array<String>) {
  DrServer.apply {
    schema = SParser.parse(User::class, Role::class, Auction::class)
    qEngine = QueryEngine(TestQueryAdaptor(), TestQueryAuthorizer())
    mEngine = ModificationEngine(TestModificationAdaptor())

    start(8080)
  }

  DrServer.schema.print()

  println("Q1")
  DrServer.qEngine.compile("""dr.User | name == "Mica*" | limit 10 {
    (asc 1) name
  }""".trimIndent())

  println("Q2")
  DrServer.qEngine.compile("""dr.User | name == "Mica*" | limit 10 page 2 { * }""".trimIndent())

  println("Q3")
  DrServer.qEngine.compile("""dr.User | name == "Mica*" | {
    name, email,
    address {
      country, city
    },
    roles | name == "admin" and order == 10 | { * }
  }""".trimMargin())

  println("Q4")
  DrServer.qEngine.compile("""dr.User | name == "Mica*" and roles..order == 1 | { * }""")

  println("Q5")
  val query = DrServer.qEngine.compile("""dr.User |  email == "email" and (name == "Mica*" or roles..name == ?name) | { * }""")
  query.exec(mapOf("name" to "admin"))

  val userId = DrServer.mEngine.create(
    User(
      name = "Micael",
      email = "email@gmail.com",
      market = Pair(1L, Traits(Trace(LocalDateTime.now()))),
      address = 1L,
      roles = mapOf(1L to Traits(Trace(LocalDateTime.now())), 2L to Traits(Trace(LocalDateTime.now())))
    )
  )

  DrServer.mEngine.update(User::class.qualifiedName!!, userId, mapOf(
    "name" to "Micael",
    "email" to "email@gmail.com"
  ))

  val auction = DrServer.mEngine.create(
    Auction(
      name = "Continente",
      items = setOf(1L, 2L),
      bids = listOf(Bid(price = 10F, boxes = 10, from = userId, item = 1L))
    )
  )
}
