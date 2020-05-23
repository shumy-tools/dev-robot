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

@Trait
data class EmbeddedAddress(val country: String, val city: String)

@Master
class OwnedUserType (
  val boss: String,
  @Own val user: Pack<UserType>
) {
  lateinit var inputOrDerived: String

  @LateInit // native init is invoked by jackson to soon
  fun late() {
    if (!this::inputOrDerived.isInitialized) {
      inputOrDerived = "default-value"
    }
  }
}

@Detail @Sealed(Customer::class, Supplier::class)
class UserType(
  val user: String,
  @Checks(EmailCheck::class) val email: String,

  val password: String,
  @Link(Role::class) val roles: List<RefID>
)

  @Detail
  class Customer(
    val discount: Float,
    @Own val address: EmbeddedAddress
  )

  @Detail
  class Supplier(@Own val organization: Organization)

@Detail
class Organization(val name: String, val address: String)

@Master
class Sell (
  val price: Float,
  @Link(Customer::class, traits = [EmbeddedAddress::class]) val customer: Traits
)

// model-2 ----------------------------------------------------------------------------------
@Trait
data class Trace(val date: LocalDateTime)

@Trait
data class Trace2(val date: LocalDateTime)

@Trait
data class UserMarket(
  @Own val trace: Trace,         // Trace.date overrides UserMarket.date
  @Link(User::class) val boss: RefID
) {
  val date: LocalDateTime = LocalDateTime.now()
}

@Master
data class Market(val name: String)

@Master
data class User(
        @Unique val name: String,

        @Checks(EmailCheck::class) val email: String,

        @Own val address: List<Address>,

  //@Create val address: Address,

  //@Link(Market::class, traits = [UserMarket::class]) val market: Traits,

        @Link(Role::class, traits = [Trace::class]) val roles: List<Traits>
  //@Link(Role::class) val roles: List<RefID>
) {
  val timestamp = LocalDateTime.now()
}

@Detail
data class Address(
  val country: String,
  val city: String,
  val address: String?
) {
  val derived = "derived-value"
}

@Master
data class Role(
  val name: String,
  val ord: Int
)

@Master
data class Auction(
  val name: String,

  @Link(AuctionItem::class) val items: List<RefID>,
  @Own val bids: List<Bid>
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

        @Own val detail: BidDetail,
        @Link(User::class) val from: RefID,
        @Link(AuctionItem::class) val item: RefID
) {
  val timestamp = LocalDateTime.now()
}

@Detail
data class BidDetail(
  val some: String
)