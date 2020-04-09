package dr

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dr.modification.ModificationEngine
import dr.query.QTree
import dr.query.QueryEngine
import dr.schema.SParser
import dr.schema.Traits
import dr.schema.print
import dr.spi.*
import java.time.LocalDateTime
import java.time.Month

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

  override fun commit(instructions: Instructions): List<Long> {
    println("TX-START")
    val ids = instructions.exec {
      when (it) {
        is Insert -> ++idSeq
        is Update -> it.id
        is Delete -> it.id
      }
    }
    println("TX-COMMIT")
    return ids
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
      market = Pair(1L, Traits(UserMarket(Trace(LocalDateTime.now()),1L))),
      address = Address("Portugal", "Aveiro", "Some street and number"),
      roles = mapOf(1L to Traits(Trace(LocalDateTime.now())), 2L to Traits(Trace(LocalDateTime.now())))
    )
  )
  println("USER-ID: $userId")

  println("")
  DrServer.mEngine.update(User::class.qualifiedName!!, userId, mapOf(
    "name" to "Micael",
    "email" to "email@gmail.com",
    "market" to Pair(5L, Traits(UserMarket(Trace(LocalDateTime.of(2000, Month.JANUARY, 1, 12, 0,0)),2L)))
  ))

  println("")
  val auctionId = DrServer.mEngine.create(
    Auction(
      name = "Continente",
      items = setOf(1L, 2L, 3L),
      bids = listOf(
        Bid(price = 10F, boxes = 10, from = userId, item = 1L, detail = BidDetail("detail-1")),
        Bid(price = 13F, boxes = 5, from = userId, item = 2L, detail = BidDetail("detail-2"))
      )
    )
  )
  println("AUCTION-ID: $auctionId")

  println("")
  val bidId = DrServer.mEngine.add(Auction::class.qualifiedName!!, auctionId, "bids", Bid(price = 23F, boxes = 1, from = userId, item = 3L, detail = BidDetail("detail-3")))
  println("BID-ID: $bidId")

  println("")
  val roles = mapOf(6L to Traits(Trace(LocalDateTime.now())), 7L to Traits(Trace(LocalDateTime.now())))
  val roleId = DrServer.mEngine.link(User::class.qualifiedName!!, userId, "roles", roles)
  println("ROLE-ID: $roleId")
}
