package dr

import dr.schema.*
import java.time.LocalDateTime

// reusable constraint check
class EmailCheck: FieldCheck<String> {
  override fun check(value: String): String? {
    return if (!value.contains('@')) "invalid email" else null
  }
}

// model-1 ----------------------------------------------------------------------------------
// TODO: add support to include embedded traits or details? (at the moment Address is owned by User)

@Master @Sealed(Customer::class, Supplier::class) // @Sealed adds type field
class UserType(
  val user: String,
  @Checks(EmailCheck::class) val email: String,
  val password: String,
  @Link(Role::class) val roles: Set<Long>
)

  @Detail
  class Customer(val address: String)

  @Detail
  class Supplier(@Create val organization: Organization)

@Detail
class Organization(val name: String, val address: String)

@Master
class Sell (
  val price: Float,
  @Link(Customer::class) val customer: Long
)

// model-2 ----------------------------------------------------------------------------------
@Trait
data class Trace(val date: LocalDateTime)

@Trait
data class UserMarket(
  @Create val trace: Trace,         // Trace.date overrides UserMarket.date
  @Link(User::class) val boss: Long
) {
  val date: LocalDateTime = LocalDateTime.now()
}

@Master
data class Market(val name: String)

@Master
data class User(
  val name: String,
  @Checks(EmailCheck::class) val email: String,

  @Create val address: Address,

  @Open @Link(Market::class, traits = [UserMarket::class]) val market: Pair<Long, Pack>,
  @Open @Link(Role::class, traits = [Trace::class]) val roles: Map<Long, Pack>
) {
  val timestamp = LocalDateTime.now()
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

  @Link(AuctionItem::class) val items: Set<Long>,
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

  @Create val detail: BidDetail,
  @Link(User::class) val from: Long,
  @Link(AuctionItem::class) val item: Long
) {
  val timestamp = LocalDateTime.now()
}

@Detail
data class BidDetail(
  val some: String
)