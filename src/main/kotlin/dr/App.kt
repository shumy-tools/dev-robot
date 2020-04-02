package dr

import dr.schema.*
import dr.query.*

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
  
  @Open @Link(Trace::class) val roles: List<Role>
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

class TestQueryExecutor(): QueryExecutor {
  override fun exec() {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
  }
}

class TestQueryAdaptor(): QueryAdaptor {
  override fun compile(query: CompiledQuery): QueryExecutor {
    println("Adaptor: $query")
    return TestQueryExecutor()
  }
}

fun main(args: Array<String>) {
  val schema = SchemaParser.parse(User::class, Role::class, Auction::class)
  schema.print()

  val qa = TestQueryAdaptor()
  val qe = DrQueryEngine(schema, qa)

  println("Q1")
  qe.compile("""dr.User | name == "Mica*" | limit 10 { * }""")

  println("Q2")
  qe.compile("""dr.User | name == "Mica*" | {
    name, birthday,
    address { * },
    roles | order > 1 | { name }
  }""".trimMargin())
}
