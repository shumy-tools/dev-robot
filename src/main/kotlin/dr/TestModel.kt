package dr

import dr.schema.*
import dr.state.Machine
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
  @Create val users: Pack<UserType>
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
  @Link(Role::class) val roles: List<Long>
)

  @Detail
  class Customer(
    val discount: Float,
    @Create val address: EmbeddedAddress
  )

  @Detail
  class Supplier(@Create val organization: Organization)

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
  @Create val trace: Trace,         // Trace.date overrides UserMarket.date
  @Link(User::class) val boss: Long
) {
  val date: LocalDateTime = LocalDateTime.now()
}

@Master
data class Market(val name: String)

@Master @StateMachine(UserMachine::class)
data class User(
  val name: String,

  @Checks(EmailCheck::class) val email: String,

  @Create val address: Address,

  //@Link(Market::class, traits = [UserMarket::class]) val market: Traits,

  //@Unique @Link(Role::class, traits = [Trace::class]) val roles: List<Traits>,

  @Unique @Link(Role::class) val roles: List<Long>
) {
  val timestamp = LocalDateTime.now()
}


class UserMachine: Machine<UserMachine.State, UserMachine.Event>() {
  enum class State { START, VALIDATE, STOP }

  sealed class Event {
    data class Submit(val value: String): Event()
    object Ok: Event()
    object Incorrect: Event()
  }

  val q1 = query("""dr.User | name == "Mica*" | {*}""")

  init {
    enter(State.START) {
      println("START")
      open(User::address).forRole("employee")
    }

    on(State.START, Event.Submit::class) fromRole "employee" goto State.VALIDATE after {
      check { event.value.startsWith("#") }

      println("(SUBMIT(${event.value}) from 'employee') START -> VALIDATE")
      history.set("owner", user)
      close(User::address).forAll()
    }

    on(State.VALIDATE, Event.Ok::class) fromRole "manager" goto State.STOP after {
      val record = history.last(Event.Submit::class)
      println("OK -> check(${record.event.value})")

      println("(OK from 'manager') VALIDATE -> STOP")
    }

    on(State.VALIDATE, Event.Incorrect::class) fromRole "manager" goto State.START after {
      val record = history.last(Event.Submit::class)
      println("INCORRECT -> check(${record.event.value})")

      println("(INCORRECT from 'manager') VALIDATE -> START")
      val owner: User = history.last(Event.Submit::class).get("owner")
      open(User::address).forUser(owner.name)
    }

    enter(State.STOP) {
      println("STOP")
    }
  }
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
  val order: Int
)

@Master
data class Auction(
  val name: String,

  @Link(AuctionItem::class) val items: List<Long>,
  @Create val bids: List<Bid>
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