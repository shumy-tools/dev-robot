package dr

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dr.schema.*
import dr.query.*
import dr.spi.IAccessed
import dr.spi.IQueryAdaptor
import dr.spi.IQueryAuthorize
import dr.spi.IQueryExecutor

import java.time.LocalDateTime

@Trait
class Trace(
  val date: LocalDateTime//,
  //val user: User
)

@Master
class User(
  val name: String,
  val email: String,

  @Link val address: Address,
  @Open @Link(Trace::class) val roles: List<Role>
)

@Detail
class Address(
  val country: String,
  val city: String,
  val address: String
)

@Master
class Role(
  val name: String
)

@Master
class Auction(
  val name: String,

  @Create val items: List<AuctionItem>,
  @Open @Create val bids: List<Bid>
)

@Detail
class AuctionItem(
  val name: String,
  val price: Float) {
}

@Detail
class Bid(
  val price: Float,
  val boxes: Int,
  val comments: String?,
  
  @Link val from: User,
  @Create val item: AuctionItem
)

val mapper: ObjectWriter = jacksonObjectMapper()
  .setSerializationInclusion(JsonInclude.Include.NON_NULL)
  .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
  .writerWithDefaultPrettyPrinter()

class TestQueryExecutor(): IQueryExecutor {
  override fun exec() {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
  }
}

class TestQueryAdaptor(): IQueryAdaptor {
  override fun compile(query: QTree): IQueryExecutor {
    val json = mapper.writeValueAsString(query);

    println("tree = $json")
    return TestQueryExecutor()
  }
}

class TestQueryAuthorize(): IQueryAuthorize {
  override fun authorized(accessed: IAccessed): Boolean {
    println("accessed = $accessed")
    return true
  }
}

fun main(args: Array<String>) {
  val schema = SParser.parse(User::class, Role::class, Auction::class)

  val qAdaptor = TestQueryAdaptor()
  val qAuthorize = TestQueryAuthorize()
  val qEngine = QEngine(schema, qAdaptor, qAuthorize)

  println("Q1")
  qEngine.compile("""dr.User | name == "Mica*" | limit 10 {
    (asc 1) name
  }""".trimIndent())

  println("Q2")
  qEngine.compile("""dr.User | name == "Mica*" | limit 10 page 2 { * }""".trimIndent())

  println("Q3")
  qEngine.compile("""dr.User | name == "Mica*" | {
    name, email,
    address {
      country, city
    },
    roles | name == "admin" | { * }
  }""".trimMargin())

  println("Q4")
  qEngine.compile("""dr.User | name == "Mica*" and roles..name == "admin" | { * }""")

  println("Q5")
  qEngine.compile("""dr.User |  email == "email" and (name == "Mica*" or roles..name == "admin") | { * }""")
}
