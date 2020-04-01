package dr

import dr.schema.*
import java.time.LocalDateTime

@Link
class Trace(
  val date: LocalDateTime
)

@Master
class User(
  val name: String,
  val email: String,
  @Open @Aggregation(Trace::class) val roles: List<Role>
)

@Master
class Role(
  val name: String
)

@Master
class Auction(
  val name: String,
  @Composition val items: List<AuctionItem>,
  @Open @Composition val bids: List<Bid>
) {
}

@Item
class AuctionItem(
  val name: String,
  val price: Float) {
}

@Item
class Bid(
  val from: User,
  val item: AuctionItem,
  val price: Float,
  val boxes: Int,
  val comments: String?
) {
}

fun main(args: Array<String>) {
  val schema = SchemaParser.parse(User::class, Role::class, Auction::class)
  schema.print()
}
