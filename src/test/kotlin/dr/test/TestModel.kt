package dr.test

import dr.base.User
import dr.schema.*
import dr.state.Machine
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

private val fixedTimestamp = LocalDateTime.of(1918, 1, 10, 12, 35, 18)

/* -------------------------------Simple Input Model------------------------------- */
@Trait
data class Trace(val value: String)

@Master
data class A(
  val oneText: String,
  val twoInt: Int,
  val threeLong: Long,
  val fourFloat: Float,
  val fiveDouble: Double,
  val sixBoolean: Boolean,
  val sevenTime: LocalTime,
  val eightDate: LocalDate,
  val nineDateTime: LocalDateTime
) {
  val timestamp = fixedTimestamp
}

@Master
data class BRefs(
  val brefsText: String?,
  @Own val crefDetail: CRefDetail?,
  @Link(CMaster::class) val cref: RefID?,
  @Link(CMaster::class, Trace::class) val crefTraits: Traits?
)

@Master
data class BCols(
  val bcolsText: String,
  @Own val ccolDetail: List<CColDetail>,
  @Link(CMaster::class) val ccol: List<RefID>,
  @Link(CMaster::class, Trace::class) val ccolTraits: List<Traits>,

  @Unique @Link(CMaster::class) val ccolUnique: List<RefID>
)

@Detail
data class CRefDetail(val crefDetailText: String)

@Detail
data class CColDetail(val ccolDetailText: String)

@Master
data class CMaster(val cmasterText: String)

/* -------------------------------Simple Query Model------------------------------- */
@Master
data class Country(val name: String)

@Detail
data class Address(
  @Link(Country::class) val country: RefID,
  val city: String,
  val location: String?
)

@Detail
data class Setting(
  @Unique val key: String,
  val value: String
)

@Master
data class User(
  @Unique val name: String,
  val email: String,
  @Own val address: Address,

  @Own val settings: List<Setting>,
  @Link(Role::class, traits = [Trace::class]) val roles: List<Traits>
) {
  val timestamp = fixedTimestamp
}

@Master
data class Role(
  val name: String,
  val ord: Int
)

/* -------------------------------Hierarchy Query Model------------------------------- */
@Master
data class RefsToPack(
  @Own val ownedAdmin: Pack<OwnedSuperUser>?,
  @Link(SuperUser::class) val admin: RefID,
  @Link(OperUser::class) val oper: RefID
)

@Master @Sealed(AdminUser::class, OperUser::class)
data class SuperUser(
  @Unique val alias: String
)

@Detail
data class AdminUser(
  val adminProp: String
)

@Detail
data class OperUser(
  val operProp: String
)

@Detail @Sealed(OwnedAdminUser::class, OwnedOperUser::class)
data class OwnedSuperUser(
  @Unique val ownedAlias: String
)

@Detail
data class OwnedAdminUser(
  val ownedAdminProp: String
)

@Detail
data class OwnedOperUser(
  val ownedOperProp: String
)

/* -------------------------------State Machine Model------------------------------- */
@Master
data class RefToMEntity(
  val alias: String,
  @Link(MEntity::class) val to: RefID
)

@Master @StateMachine(dr.test.MEntityMachine::class)
data class MEntity(
  @Unique val name: String
)

class MEntityMachine: Machine<MEntity, MEntityMachine.State, MEntityMachine.Event>() {
  enum class State { START, VALIDATE, STOP }

  sealed class Event {
    data class Submit(val value: String): Event()
    object Ok: Event()
    object Incorrect: Event()
  }

  private val q1 = query("""dr.base.User | name == "shumy" | { * }""")

  init {
    onCreate = { id, entity ->
      assert(entity.name == "My Name")
      assert(q1.exec().rows.toString() == "[{@id=1, name=shumy, email=shumy@gmail.com}]")
      create(RefToMEntity("m-alias", id))
    }

    onUpdate = {
      println("UPDATE: ${it.id} - ${it.get(MEntity::name)}")
    }

    enter(State.START) {
      println("START")
      open(MEntity::name).forRole("admin")
    }

    on(State.START, Event.Submit::class) fromRole "admin" goto State.VALIDATE after {
      check { event.value.startsWith("#") }

      println("(SUBMIT(${event.value}) from 'employee') START -> VALIDATE")
      history.set("owner", user)
      close(MEntity::name).forAll()
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
      open(MEntity::name).forUser(owner.name)
    }

    enter(State.STOP) {
      println("STOP")
    }
  }
}