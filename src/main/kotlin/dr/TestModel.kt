package dr

import dr.schema.*
import java.time.LocalDateTime

// reusable constraint check
class EmailCheck: FieldCheck<String> {
  override fun check(value: String): String? {
    return if (!value.contains('@')) "invalid email" else null
  }
}

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

@Master @Listeners(UserListener::class)
data class User(
  val name: String,
  @Checks(EmailCheck::class) val email: String,

  @Create val address: Address,

  @Open @Link(Market::class, traits = [UserMarket::class]) val market: Pair<Long, Traits>,
  @Open @Link(Role::class, traits = [Trace::class]) val roles: Map<Long, Traits>
) {
  val timestamp = LocalDateTime.now()
}
  // process events and check business rules (can plug a rule engine)
  class UserListener: EListener<User>() {
    @Events(EventType.CHECKED, EventType.COMMITED)
    override fun onCreate(type: EventType, id: Long?, new: User) {
      println("CREATE-User($type) - ($id, $new)")
    }

    @Events(EventType.CHECKED)
    override fun onUpdate(type: EventType, id: Long, data: Map<String, Any?>) {
      println("UPDATE-User($type) - ($id, $data)")
    }
  }

@Detail  @Listeners(AddressListener::class)
data class Address(
  val country: String,
  val city: String,
  val address: String
)
  // process events and check business rules (can plug a rule engine)
  class AddressListener: EListener<Address>() {
    @Events(EventType.CHECKED, EventType.COMMITED)
    override fun onCreate(type: EventType, id: Long?, new: Address) {
      println("CREATE-Address($type) - ($id, $new)")
    }

    @Events(EventType.CHECKED)
    override fun onUpdate(type: EventType, id: Long, data: Map<String, Any?>) {
      println("UPDATE-Address($type) - ($id, $data)")
    }

    @Events(EventType.CHECKED)
    override fun onLink(type: EventType, id: Long?, sRelation: SRelation, link: Long) {
      println("LINK-Address($type) - ($id, ${sRelation.name}, $link)")
    }
  }

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