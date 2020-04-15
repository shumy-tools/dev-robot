package dr.test

import dr.io.*
import dr.schema.*
import org.junit.Test
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
  @Open @Link(C::class) val threeEntity: Long,
  @Open @Link(C::class, Trace::class) val fourEntity: Traits
)

@Master
data class B1(
  val oneText: String,
  @Open @Link(C::class) val twoEntity: List<Long>,
  @Open @Link(C::class, Trace::class) val threeEntity: List<Traits>
)

@Detail
data class C(val oneText: String)

private val schema = SParser.parse(A::class, B::class, B1::class)

class InputProcessorTest {
  private val ip = InputProcessor(schema)

  private val fieldInputs = """{
    "oneText":"one",
    "twoInt":2,
    "threeLong":3,
    "fourFloat":4.0,
    "fiveDouble":5.0,
    "sixBoolean":true,
    "sevenTime":"10:30:20",
    "eightDate":"2020-01-25",
    "nineDateTime":"2020-01-25T10:30:20"
  }"""

  @Test fun testCreateFieldTypes() {
    val entity = ip.create(A::class, fieldInputs)
    assert(entity.toMap() == mapOf(
      "oneText" to "one",
      "twoInt" to 2,
      "threeLong" to 3L,
      "fourFloat" to 4.0F,
      "fiveDouble" to 5.0,
      "sixBoolean" to true,
      "sevenTime" to time,
      "eightDate" to date,
      "nineDateTime" to datetime,
      "timestamp" to fixedTimestamp
    ))
  }

  @Test fun testUpdateFieldTypes() {
    val entity = ip.update(A::class, fieldInputs)
    assert(entity.toMap() == mapOf(
      "oneText" to "one",
      "twoInt" to 2L,
      "threeLong" to 3L,
      "fourFloat" to 4.0,
      "fiveDouble" to 5.0,
      "sixBoolean" to true,
      "sevenTime" to time,
      "eightDate" to date,
      "nineDateTime" to datetime
    ))
  }

  @Test fun testCreateWithReferences() {
    val json = """{
      "oneText":"oneB",
      "twoEntity":{
        "oneText":"oneC"
      },
      "threeEntity":1,
      "fourEntity":{
        "id":2,
        "traits":[{"@type":"dr.test.Trace","value":"traceValue"}]
      }
    }"""

    val entity = ip.create(B::class, json)
    assert(entity.toMap() == mapOf(
      "oneText" to "oneB",
      "twoEntity" to mapOf("oneText" to "oneC"),
      "threeEntity" to OneLinkWithoutTraits(1L),
      "fourEntity" to OneLinkWithTraits(Traits(2L, Trace("traceValue")))
    ))
  }

  @Test fun testLinkRelations() {
    val json = """{
      "twoEntity":{
        "oneText":"oneC"
      },
      "threeEntity":{
        "@type":"one-link",
        "ref":100
      },
      "fourEntity":{
        "@type":"one-link-traits",
        "ref":{
          "id":200,
          "traits":[{"@type":"dr.test.Trace","value":"traceUpdate"}]}
      }
    }"""

    val entity = ip.update(B::class, json)
    assert(entity.toMap() == mapOf(
      "twoEntity" to mapOf("oneText" to "oneC"),
      "threeEntity" to OneLinkWithoutTraits(100L),
      "fourEntity" to OneLinkWithTraits(Traits(200L, Trace("traceUpdate")))
    ))
  }

  @Test fun testUnlinkRelations() {
    val json = """{
      "twoEntity":{
        "@type":"one-unlink",
        "ref":100
      },
      "threeEntity":{
        "@type":"many-unlink",
        "refs":[200,300]
      }
    }"""

    val entity = ip.update(B1::class, json)
    assert(entity.toMap() == mapOf(
      "twoEntity" to OneUnlink(100L),
      "threeEntity" to ManyUnlink(200L, 300L)
    ))
  }
}