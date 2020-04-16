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

private fun Instructions.process(): List<Instruction> {
  var idSeq = 9L;
  this.exec {
    val id = when (it) {
      is Insert -> ++idSeq
      is Update -> it.id
      is Delete -> 0L
    }

    println("$id - $it")
    id
  }

  return this.all
}

class InputTest {
  private val ip = InputProcessor(schema)
  private val trans = DEntityTranslator(schema)

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
    val allInst = trans.create(entity).process()
    assert(allInst.size == 1)

    allInst[0].isCreate(A::class, "dr_test_a", mapOf(
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
    val allInst = trans.update(10L, entity).process()
    assert(allInst.size == 1)

    allInst[0].isUpdate(A::class, 10L, "dr_test_a", mapOf(
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
    val allInst = trans.create(entity).process()
    assert(allInst.size == 2)

    allInst[0].isCreate(C::class, "dr_test_c", mapOf(
      "oneText" to "oneC"
    ))

    allInst[1].isCreate(B::class, "dr_test_b",
      mapOf(
        "oneText" to "oneB",
        "traits__fourEntity" to mapOf("value" to "traceValue")
      ),
      mapOf(
        "ref__threeEntity" to 1L,
        "ref__fourEntity" to 2L,
        "ref__twoEntity" to 10L
      )
    )
  }

  @Test fun testCreateWithCollections() {
    val json = """{
      "oneText":"oneB1",
      "twoEntity":[1, 2],
      "threeEntity":[
        {"id":100,"traits":[{"@type":"dr.test.Trace","value":"trace1"}]},
        {"id":200,"traits":[{"@type":"dr.test.Trace","value":"trace2"}]}
      ]
    }"""

    val entity = ip.create(B1::class, json)
    val allInst = trans.create(entity).process()
    assert(allInst.size == 5)

    allInst[0].isCreate(B1::class, "dr_test_b1", mapOf(
      "oneText" to "oneB1"
    ))

    allInst[1].isLink(B1::class, "twoEntity", "dr_test_b1__twoEntity", emptyMap(),
      mapOf("ref" to 1L, "inv" to 10L)
    )

    allInst[2].isLink(B1::class, "twoEntity", "dr_test_b1__twoEntity", emptyMap(),
      mapOf("ref" to 2L, "inv" to 10L)
    )

    allInst[3].isLink(B1::class, "threeEntity", "dr_test_b1__threeEntity",
      mapOf("traits__threeEntity" to mapOf("value" to "trace1")),
      mapOf("ref" to 100L, "inv" to 10L)
    )

    allInst[4].isLink(B1::class, "threeEntity", "dr_test_b1__threeEntity",
      mapOf("traits__threeEntity" to mapOf("value" to "trace2")),
      mapOf("ref" to 200L, "inv" to 10L)
    )
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
    val allInst = trans.update(20L, entity).process()
    assert(allInst.size == 2)

    allInst[0].isCreate(C::class, "dr_test_c", mapOf(
      "oneText" to "oneC"
    ))

    allInst[1].isUpdate(B::class, 20L, "dr_test_b",
      mapOf("traits__fourEntity" to mapOf("value" to "traceUpdate")),
      mapOf("ref__twoEntity" to 10L, "ref__threeEntity" to 100L, "ref__fourEntity" to 200L)
    )
  }

  @Test fun testUnlinkRelations() {
    val json = """{
      "twoEntity":{
        "@type":"one-unlink",
        "ref":100
      },
      "threeEntity":{
        "@type":"many-unlink",
        "refs":[200, 300]
      }
    }"""

    val entity = ip.update(B1::class, json)
    val allInst = trans.update(20L, entity).process()
    assert(allInst.size == 4)

    allInst[0].isUpdate(B1::class, 20L, "dr_test_b1", emptyMap())

    allInst[1].isUnlink(B1::class, "twoEntity", "dr_test_b1__twoEntity",
      mapOf("ref" to 100L, "inv" to 20L)
    )

    allInst[2].isUnlink(B1::class, "threeEntity", "dr_test_b1__threeEntity",
      mapOf("ref" to 200L, "inv" to 20L)
    )

    allInst[3].isUnlink(B1::class, "threeEntity", "dr_test_b1__threeEntity",
      mapOf("ref" to 300L, "inv" to 20L)
    )
  }
}