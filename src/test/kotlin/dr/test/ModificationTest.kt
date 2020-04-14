package dr.test

import dr.JsonEngine
import dr.modification.ManyLinkDelete
import dr.modification.OneLinkDelete
import dr.modification.OneLinkWithTraits
import dr.modification.OneLinkWithoutTraits
import dr.schema.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.test.Test

private val time = LocalTime.of(10, 30, 20)
private val date = LocalDate.of(2020, 1, 25)
private val datetime = LocalDateTime.of(date, time)

@Trait
class Trace(val value: String)

@Master
class A(
  val oneText: String,
  val twoInt: Int,
  val threeLong: Long,
  val fourFloat: Float,
  val fiveDouble: Double,
  val sixBoolean: Boolean,
  val sevenTime: LocalTime,
  val eightDate: LocalDate,
  val nineDateTime: LocalDateTime
)

@Master
class B(
  val oneText: String,
  @Open @Create val twoEntity: C,
  @Open @Link(C::class) val threeEntity: Long,
  @Open @Link(C::class, Trace::class) val fourEntity: Traits
)

@Master
class B1(
  val oneText: String,
  @Open @Link(C::class) val twoEntity: List<Long>,
  @Open @Link(C::class, Trace::class) val threeEntity: List<Traits>
)

@Detail
class C(val oneText: String)

// inst.list.forEach { println(it) }
private val schema = SParser.parse(A::class, B::class, B1::class)

class ModificationTest {
  @Test fun testCreateFieldTypes() {
    val value = A("one", 2, 3L, 4.0F, 5.0, true, time, date, datetime)

    val mEngine = TestHelper.modification(schema) { inst ->
      val i0 = inst.list[0]
      i0.isCreate(A::class, "dr_test_a", mapOf(
        "oneText" to "one",
        "twoInt" to 2,
        "threeLong" to 3L,
        "fourFloat" to 4.0F,
        "fiveDouble" to 5.0,
        "sixBoolean" to true,
        "sevenTime" to time,
        "eightDate" to date,
        "nineDateTime" to datetime
      ))
    }

    val json = JsonEngine.serialize(value)
    assert(json == """{"oneText":"one","twoInt":2,"threeLong":3,"fourFloat":4.0,"fiveDouble":5.0,"sixBoolean":true,"sevenTime":"10:30:20","eightDate":"2020-01-25","nineDateTime":"2020-01-25T10:30:20"}""")

    val id = mEngine.create(JsonEngine.deserialize(json, A::class))
    assert(id == 10L)
  }

  @Test fun testUpdateFieldTypes() {
    val value = mapOf(
      "oneText" to "one",
      "twoInt" to 2,
      "threeLong" to 3L,
      "fourFloat" to 4.0F,
      "fiveDouble" to 5.0,
      "sixBoolean" to true,
      "sevenTime" to time,
      "eightDate" to date,
      "nineDateTime" to datetime
    )

    val mEngine = TestHelper.modification(schema) { inst ->
      val i0 = inst.list[0]
      i0.isUpdate(A::class, 10L, "dr_test_a", mapOf(
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

    val json = JsonEngine.serialize(value)
    assert(json == """{"oneText":"one","twoInt":2,"threeLong":3,"fourFloat":4.0,"fiveDouble":5.0,"sixBoolean":true,"sevenTime":"10:30:20","eightDate":"2020-01-25","nineDateTime":"2020-01-25T10:30:20"}""")

    mEngine.update(A::class.qualifiedName!!, 10L, JsonEngine.deserialize(json, schema.find(A::class)))
  }

  @Test fun testCreateWithReferences() {
    val value = B("oneB", C("oneC"), 1L, Traits(2L, Trace("traceValue")))

    val mEngine = TestHelper.modification(schema) { inst ->
      val i0 = inst.list[0]
      val i1 = inst.list[1]

      i0.isCreate(C::class, "dr_test_c", mapOf("oneText" to "oneC"))
      i1.isCreate(B::class, "dr_test_b", mapOf("oneText" to "oneB", "traits__fourEntity" to mapOf("value" to "traceValue")),
        mapOf("ref__fourEntity" to 2L, "ref__threeEntity" to 1L, "ref__twoEntity" to 10L)
      )
    }

    val json = JsonEngine.serialize(value)
    assert(json == """{"oneText":"oneB","twoEntity":{"oneText":"oneC"},"threeEntity":1,"fourEntity":{"id":2,"traits":[{"@type":"dr.test.Trace","value":"traceValue"}]}}""")

    val id = mEngine.create(JsonEngine.deserialize(json, B::class))
    assert(id == 11L)
  }

  @Test fun testUpdateWithReferences() {
    val value = mapOf(
      "twoEntity" to C("oneC"),
      "threeEntity" to mapOf("@type" to "one-link", "ref" to 100L),
      "fourEntity" to mapOf("@type" to "one-link-traits", "ref" to Traits(200L, Trace("traceUpdate")))
    )

    val mEngine = TestHelper.modification(schema) { inst ->
      val i0 = inst.list[0]
      val i1 = inst.list[1]

      i0.isCreate(C::class, "dr_test_c", mapOf("oneText" to "oneC"))
      i1.isUpdate(B::class, 11L, "dr_test_b", mapOf("traits__fourEntity" to mapOf("value" to "traceUpdate")),
        mapOf("ref__fourEntity" to 200L, "ref__threeEntity" to 100L, "ref__twoEntity" to 10L)
      )
    }

    val json = JsonEngine.serialize(value)
    assert(json == """{"twoEntity":{"oneText":"oneC"},"threeEntity":{"@type":"one-link","ref":100},"fourEntity":{"@type":"one-link-traits","ref":{"id":200,"traits":[{"@type":"dr.test.Trace","value":"traceUpdate"}]}}}""")

    mEngine.update(B::class.qualifiedName!!, 11L, JsonEngine.deserialize(json, schema.find(B::class)))
  }

  @Test fun testLinkRelations() {
    val value1 = mapOf("@type" to "one-link", "ref" to 100L)
    val value2 = mapOf("@type" to "one-link-traits", "ref" to Traits(200L, Trace("traceUpdate")))

    val json1 = JsonEngine.serialize(value1)
    assert(json1 == """{"@type":"one-link","ref":100}""")

    val json2 = JsonEngine.serialize(value2)
    assert(json2 == """{"@type":"one-link-traits","ref":{"id":200,"traits":[{"@type":"dr.test.Trace","value":"traceUpdate"}]}}""")

    val mEngine1 = TestHelper.modification(schema) { inst ->
      val i0 = inst.list[0]
      i0.isLink(B1::class, "twoEntity", "dr_test_b1__twoEntity", emptyMap(), mapOf("ref" to 100L, "inv" to 10L))
    }
    mEngine1.link(B1::class.qualifiedName!!, 10L, "twoEntity", JsonEngine.deserialize(json1, OneLinkWithoutTraits::class))

    val mEngine2 = TestHelper.modification(schema) { inst ->
      val i0 = inst.list[0]
      i0.isLink(B1::class, "threeEntity", "dr_test_b1__threeEntity", mapOf("value" to "traceUpdate"), mapOf("ref" to 200L, "inv" to 10L))
    }
    mEngine2.link(B1::class.qualifiedName!!, 10L, "threeEntity", JsonEngine.deserialize(json2, OneLinkWithTraits::class))
  }

  @Test fun testUnlinkRelations() {
    val value1 = mapOf("@type" to "one-unlink", "link" to 100L)
    val value2 = mapOf("@type" to "many-unlink", "links" to listOf(200L, 300L))

    val json1 = JsonEngine.serialize(value1)
    assert(json1 == """{"@type":"one-unlink","link":100}""")

    val json2 = JsonEngine.serialize(value2)
    assert(json2 == """{"@type":"many-unlink","links":[200,300]}""")

    val mEngine1 = TestHelper.modification(schema) { inst ->
      val i0 = inst.list[0]
      i0.isUnlink(B1::class, "twoEntity", "dr_test_b1__twoEntity", 100L)
    }
    mEngine1.unlink(B1::class.qualifiedName!!, "twoEntity", JsonEngine.deserialize(json1, OneLinkDelete::class))

    val mEngine2 = TestHelper.modification(schema) { inst ->
      val i0 = inst.list[0]
      val i1 = inst.list[1]
      i0.isUnlink(B1::class, "threeEntity", "dr_test_b1__threeEntity", 200L)
      i1.isUnlink(B1::class, "threeEntity", "dr_test_b1__threeEntity", 300L)
    }
    mEngine2.unlink(B1::class.qualifiedName!!, "threeEntity", JsonEngine.deserialize(json2, ManyLinkDelete::class))
  }
}