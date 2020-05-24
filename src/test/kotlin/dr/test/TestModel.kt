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

@Master @StateMachine(MEntityMachine::class)
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

  private val q1 = query(User::class,"""| name == "shumy" | { * }""")

  init {
    onCreate = {
      assert(q1.exec().rows.toString() == "[{@id=1, name=shumy, email=shumy@gmail.com}]")
      create(RefToMEntity("a-create", id))
      open(MEntity::name).forRole("admin")
    }

    onUpdate = {
      create(RefToMEntity("a-update", id))
    }

    enter(State.START) {
      history["d-field"] = 30

      if (history.size == 0)
        assert("(state=$state, open=$open, user=${user.name})" == "(state=START, open={name={roles={admin=true}}}, user=shumy)")
      else
        assert("(state=$state, open=$open, user=${user.name})" == "(state=START, open={name={users={shumy=true}}}, user=alex)")
    }

    on(State.START, Event.Submit::class) fromRole "admin" goto State.VALIDATE after {
      check { event.value.startsWith("#try-submit") }

      history["owner"] = user.name
      close(MEntity::name).forAny()

      assert("(state=$state, open=$open, user=${user.name})" == "(state=START, open={}, user=shumy)")
    }

    on(State.VALIDATE, Event.Ok::class) fromRole "manager" goto State.STOP after {
      val record = history.last(Event.Submit::class)
      assert(record.event!!.value.startsWith("#try-submit"))
      assert(record["owner"] == "shumy")

      assert("(state=$state, open=$open, user=${user.name})" == "(state=VALIDATE, open={}, user=alex)")
    }

    on(State.VALIDATE, Event.Incorrect::class) fromRole "manager" goto State.START after {
      val record = history.last(Event.Submit::class)
      assert(record.event!!.value.startsWith("#try-submit"))
      assert(record["owner"] == "shumy")

      val owner = record["owner"] as String
      open(MEntity::name).forUser(owner)

      assert("(state=$state, open=$open, user=${user.name})" == "(state=VALIDATE, open={name={users={shumy=true}}}, user=alex)")
    }

    enter(State.STOP) {
      create(RefToMEntity("a-stop", id))
      assert("(state=$state, open=$open, user=${user.name})" == "(state=STOP, open={}, user=alex)")
    }
  }
}