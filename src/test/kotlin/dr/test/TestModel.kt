package dr.test

import dr.schema.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime


private val time = LocalTime.of(10, 30, 20)
private val date = LocalDate.of(2020, 1, 25)
private val datetime = LocalDateTime.of(date, time)
private val fixedTimestamp = LocalDateTime.of(1918, 1, 10, 12, 35, 18)

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
data class B(
  val oneText: String,
  @Open @Create val twoEntity: C,
  @Open @Link(C::class) val threeEntity: RefID,
  @Open @Link(C::class, Trace::class) val fourEntity: Traits
)

@Master
data class B1(
  val oneText: String,
  @Open @Link(C::class) val twoEntity: List<RefID>,
  @Open @Link(C::class, Trace::class) val threeEntity: List<Traits>
)

@Detail
data class C(val oneText: String)

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
  @Create val address: Address,

  @Create val settings: List<Setting>,
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