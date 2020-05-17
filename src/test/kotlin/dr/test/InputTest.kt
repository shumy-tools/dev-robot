package dr.test

import dr.io.*
import dr.schema.*
import dr.schema.tabular.TParser
import org.junit.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

private fun Instructions.process(): List<Instruction> {
  var idSeq = 9L;
  this.exec {
    val id = when (it) {
      is Insert -> ++idSeq
      is Update -> 0L
      is Delete -> 0L
    }

    println("$id - $it")
    id
  }

  return this.all
}

private val schema = SParser.parse(A::class, B::class, B1::class)

class InputTest {
  private val ip = InputProcessor(schema)
  private val trans = InstructionBuilder(TParser(schema).transform())

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
    assert(allInst[0].toString() == "Insert(CREATE) - {table=dr.test.A, data={oneText=one, twoInt=2, threeLong=3, fourFloat=4.0, fiveDouble=5.0, sixBoolean=true, sevenTime=10:30:20, eightDate=2020-01-25, nineDateTime=2020-01-25T10:30:20, timestamp=1918-01-10T12:35:18}}")
  }

  @Test fun testUpdateFieldTypes() {
    val entity = ip.update(A::class, fieldInputs)
    val allInst = trans.update(10L, entity).process()

    assert(allInst.size == 1)
    assert(allInst[0].toString() == "Update(UPDATE) - {table=dr.test.A, id=10, data={oneText=one, twoInt=2, threeLong=3, fourFloat=4.0, fiveDouble=5.0, sixBoolean=true, sevenTime=10:30:20, eightDate=2020-01-25, nineDateTime=2020-01-25T10:30:20}}")
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
    assert(allInst[0].toString() == "Insert(ADD) - {table=dr.test.C, data={oneText=oneC}}")
    assert(allInst[1].toString() == "Insert(CREATE) - {table=dr.test.B, data={oneText=oneB, &dr.test.Trace@fourEntity=Trace(value=traceValue)}, refs={@ref-to-dr.test.B-threeEntity=1, @ref-to-dr.test.B-fourEntity=2, @ref-to-dr.test.B-twoEntity=10}}")
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
    assert(allInst[0].toString() == "Insert(CREATE) - {table=dr.test.B1, data={oneText=oneB1}}")
    assert(allInst[1].toString() == "Insert(LINK) - {table=dr.test.B1-twoEntity, refs={@ref-to-dr.test.C=1, @inv-to-dr.test.B1=10}}")
    assert(allInst[2].toString() == "Insert(LINK) - {table=dr.test.B1-twoEntity, refs={@ref-to-dr.test.C=2, @inv-to-dr.test.B1=10}}")
    assert(allInst[3].toString() == "Insert(LINK) - {table=dr.test.B1-threeEntity, data={&dr.test.Trace=Trace(value=trace1)}, refs={@ref-to-dr.test.C=100, @inv-to-dr.test.B1=10}}")
    assert(allInst[4].toString() == "Insert(LINK) - {table=dr.test.B1-threeEntity, data={&dr.test.Trace=Trace(value=trace2)}, refs={@ref-to-dr.test.C=200, @inv-to-dr.test.B1=10}}")
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
          "traits":[{"@type":"dr.test.Trace","value":"traceUpdate"}]
        }
      }
    }"""

    val entity = ip.update(B::class, json)
    val allInst = trans.update(20L, entity).process()

    assert(allInst.size == 2)
    assert(allInst[0].toString() == "Insert(ADD) - {table=dr.test.C, data={oneText=oneC}}")
    assert(allInst[1].toString() == "Update(UPDATE) - {table=dr.test.B, id=20, data={&dr.test.Trace@fourEntity=Trace(value=traceUpdate)}, refs={@ref-to-dr.test.B-threeEntity=100, @ref-to-dr.test.B-fourEntity=200, @ref-to-dr.test.B-twoEntity=10}}")
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
    assert(allInst[0].toString() == "Update(UPDATE) - {table=dr.test.B1, id=20}")
    assert(allInst[1].toString() == "Delete(UNLINK) - {table=dr.test.B1-twoEntity, refs={@ref-to-dr.test.C=100, @inv-to-dr.test.B1=20}}")
    assert(allInst[2].toString() == "Delete(UNLINK) - {table=dr.test.B1-threeEntity, refs={@ref-to-dr.test.C=200, @inv-to-dr.test.B1=20}}")
    assert(allInst[3].toString() == "Delete(UNLINK) - {table=dr.test.B1-threeEntity, refs={@ref-to-dr.test.C=300, @inv-to-dr.test.B1=20}}")
  }
}