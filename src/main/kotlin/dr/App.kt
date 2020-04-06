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

// reusable constraint check
class EmailCheck: FieldCheck<String> {
  override fun check(value: String): String? {
    return if (!value.contains('@')) "invalid email" else null
  }
}

@Trait
data class Trace(
  val date: LocalDateTime,
  @Link(User::class) val user: Long
)

@Master @Listeners(UserListener::class)
data class User(
  val name: String,
  @Checks(EmailCheck::class) val email: String,

  @Link(Address::class) val address: Long,
  @Open @Link(Role::class, traits = [Trace::class]) val roles: List<Long>
) {
  val timestamp = LocalDateTime.now()
}
  // process events and check business rules (can plug a rule engine)
  class UserListener: EListener<User>() {
    @Events(EventType.CHECKED, EventType.COMMITED)
    override fun onCreate(type: EventType, id: Long?, new: User) {
      println("CREATE($type) - ($id, $new)")
    }

    @Events(EventType.CHECKED)
    override fun onUpdate(type: EventType, id: Long, data: Map<String, Any?>) {
      println("UPDATE($type) - ($id, $data)")
    }
  }

@Detail
data class Address(
  val country: String,
  val city: String,
  val address: String
)

@Master
data class Role(
  val name: String,
  val order: Int
)

@Master
data class Auction(
  val name: String,

  @Create val items: Set<AuctionItem>,
  @Open @Create val bids: List<Bid>
) {
  val timestamp = LocalDateTime.now()
}

@Detail
data class AuctionItem(
  val name: String,
  val price: Float
) {
  val timestamp = LocalDateTime.now()
}

@Detail
data class Bid(
  val price: Float,
  val boxes: Int,
  val comments: String? = null,
  
  @Link(User::class) val from: Long,
  @Link(AuctionItem::class) val item: Long
) {
  val timestamp = LocalDateTime.now()
}


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
  override fun start(): ITransaction {
    return TestTransaction()
  }
}

class TestTransaction: ITransaction {
  override fun create(sEntity: SEntity, new: Any): Long {
    return 0L
  }

  override fun update(sEntity: SEntity, id: Long, data: Map<String, Any?>) {
  }

  override fun add(sEntity: SEntity, id: Long, sRelation: SRelation, new: Any): Long {
    return 0L
  }

  override fun link(sEntity: SEntity, id: Long, sRelation: SRelation, link: Long) {
  }

  override fun remove(sEntity: SEntity, id: Long, sRelation: SRelation, link: Long) {
  }

  override fun commit() {
  }

  override fun rollback() {
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

  val user = DrServer.mEngine.create(
    User(
      name = "Micael",
      email = "email@gmail.com",
      address = 1L,
      roles = listOf(1L, 2L)
    )
  )
  println("User: $user")

  DrServer.mEngine.update(User::class.qualifiedName!!, user.first, mapOf(
    "name" to "Micael",
    "email" to "email@gmail.com"
  ))

  val auction = DrServer.mEngine.create(
    Auction(
      name = "Continente",
      items = setOf(AuctionItem("Barco", 100.0F)),
      bids = listOf(Bid(price = 10F, boxes = 10, from = user.first, item = 1L))
    )
  )
}
