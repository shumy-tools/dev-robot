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
  
  @Create val from: User,
  @Create val item: AuctionItem
)

fun main(args: Array<String>) {
  //val schema = SchemaParser.parse(User::class, Role::class, Auction::class)
  //schema.print()

  val qe = DrQueryEngine()

  println("Q1")
  qe.query("""dr.User | name == "Mica*" | { * }""")

  println("Q2")
  qe.query("""dr.User | name == "Mica*" | {
    name, birthday,
    address { * },
    roles | order > 1 | { name }
  }""".trimMargin())
}
