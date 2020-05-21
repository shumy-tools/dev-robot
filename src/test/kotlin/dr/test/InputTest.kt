package dr.test

import dr.adaptor.SQLAdaptor
import dr.ctx.Session
import dr.io.InputProcessor
import dr.io.InstructionBuilder
import dr.io.Instructions
import dr.query.QueryService
import dr.schema.Pack
import dr.schema.SParser
import dr.schema.tabular.TParser
import dr.spi.QRow
import org.junit.FixMethodOrder
import org.junit.Test
import kotlin.reflect.KClass

private val schema = SParser.parse(A::class, BRefs::class, BCols::class, CMaster::class, SuperUser::class)
private val adaptor = SQLAdaptor(schema, "jdbc:h2:mem:InputTest").also {
  it.createSchema()
}

class TestServer {
  private val processor = InputProcessor(schema, emptyMap())
  private val translator = InstructionBuilder(TParser(schema).transform())
  private val qService = QueryService(adaptor.tables, adaptor)

  fun create(type: KClass<out Any>, json: String): Instructions {
    val entity = processor.create(type, json)
    val instructions = translator.create(entity)
    dr.ctx.Context.instructions = instructions
    adaptor.commit(instructions)
    return instructions
  }

  fun update(type: KClass<out Any>, id: Long, json: String): Instructions {
    val entity = processor.update(type, id, json)
    val instructions = translator.update(id, entity)
    dr.ctx.Context.instructions = instructions
    adaptor.commit(instructions)
    return instructions
  }

  fun query(query: String): List<QRow> {
    dr.ctx.Context.session = Session(processor, translator, qService)
    return dr.ctx.Context.query(query).exec().rows
  }
}

private val server = TestServer()

private fun createCMasters(): List<Long> {
  val cJson1 = """{
      "cmasterText":"value1"
    }"""
  val cInst1 = server.create(CMaster::class, cJson1)
  val id1 = cInst1.root.refID.id!!
  assert(cInst1.all.size == 1)
  assert(cInst1.all[0].toString() == "Insert(CREATE) - {table=dr.test.CMaster, data={cmasterText=value1}}")

  val cJson2 = """{
      "cmasterText":"value2"
    }"""
  val cInst2 = server.create(CMaster::class, cJson2)
  val id2 = cInst2.root.refID.id!!
  assert(cInst2.all.size == 1)
  assert(cInst2.all[0].toString() == "Insert(CREATE) - {table=dr.test.CMaster, data={cmasterText=value2}}")

  val cJson3 = """{
      "cmasterText":"value3"
    }"""
  val cInst3 = server.create(CMaster::class, cJson3)
  val id3 = cInst3.root.refID.id!!
  assert(cInst3.all.size == 1)
  assert(cInst3.all[0].toString() == "Insert(CREATE) - {table=dr.test.CMaster, data={cmasterText=value3}}")

  val cJson4 = """{
      "cmasterText":"value4"
    }"""
  val cInst4 = server.create(CMaster::class, cJson4)
  val id4 = cInst4.root.refID.id!!
  assert(cInst4.all.size == 1)
  assert(cInst4.all[0].toString() == "Insert(CREATE) - {table=dr.test.CMaster, data={cmasterText=value4}}")

  val cJson5 = """{
      "cmasterText":"value5"
    }"""
  val cInst5 = server.create(CMaster::class, cJson5)
  val id5 = cInst5.root.refID.id!!
  assert(cInst5.all.size == 1)
  assert(cInst5.all[0].toString() == "Insert(CREATE) - {table=dr.test.CMaster, data={cmasterText=value5}}")

  return listOf(id1, id2, id3, id4, id5)
}

private val ids = createCMasters()

@FixMethodOrder
class InputTest {
  @Test fun testDBState() {
    assert(server.query("dr.test.CMaster { * }").toString() == "[{@id=1, cmasterText=value1}, {@id=2, cmasterText=value2}, {@id=3, cmasterText=value3}, {@id=4, cmasterText=value4}, {@id=5, cmasterText=value5}]")
  }

  @Test fun testFieldTypes() {
    val cJson = """{
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
    val cInst = server.create(A::class, cJson)
    assert(cInst.all.size == 1)
    assert(cInst.all[0].toString() == "Insert(CREATE) - {table=dr.test.A, data={oneText=one, twoInt=2, threeLong=3, fourFloat=4.0, fiveDouble=5.0, sixBoolean=true, sevenTime=10:30:20, eightDate=2020-01-25, nineDateTime=2020-01-25T10:30:20, timestamp=1918-01-10T12:35:18}}")
    assert(server.query("dr.test.A { * }").toString() == "[{@id=1, oneText=one, twoInt=2, threeLong=3, fourFloat=4.0, fiveDouble=5.0, sixBoolean=true, sevenTime=10:30:20, eightDate=2020-01-25, nineDateTime=2020-01-25T10:30:20, timestamp=1918-01-10T12:35:18}]")

    val uJson = """{
      "oneText":"u-one",
      "twoInt":20,
      "threeLong":30,
      "fourFloat":40.0,
      "fiveDouble":50.0,
      "sixBoolean":false,
      "sevenTime":"10:40:25",
      "eightDate":"2019-02-12",
      "nineDateTime":"2018-03-10T10:45:35"
    }"""
    val uInst = server.update(A::class, cInst.root.refID.id!!, uJson)
    assert(uInst.all.size == 1)
    assert(uInst.all[0].toString() == "Update(UPDATE) - {table=dr.test.A, id=${cInst.root.refID.id}, data={oneText=u-one, twoInt=20, threeLong=30, fourFloat=40.0, fiveDouble=50.0, sixBoolean=false, sevenTime=10:40:25, eightDate=2019-02-12, nineDateTime=2018-03-10T10:45:35}}")
    assert(server.query("dr.test.A { * }").toString() == "[{@id=1, oneText=u-one, twoInt=20, threeLong=30, fourFloat=40.0, fiveDouble=50.0, sixBoolean=false, sevenTime=10:40:25, eightDate=2019-02-12, nineDateTime=2018-03-10T10:45:35, timestamp=1918-01-10T12:35:18}]")
  }

  @Test fun testReferences() {
    val cJson = """{
      "brefsText":"value1",
      "crefDetail":{
        "crefDetailText":"value2"
      },
      "cref":${ids[0]},
      "crefTraits":{
        "id":${ids[1]},
        "traits":[{"@type":"dr.test.Trace","value":"traceValue"}]
      }
    }"""
    val cInst = server.create(BRefs::class, cJson)
    val id = cInst.root.refID.id!!
    assert(cInst.all.size == 2)
    assert(cInst.all[0].toString() == "Insert(ADD) - {table=dr.test.CRefDetail, data={crefDetailText=value2}}")
    assert(cInst.all[1].toString() == "Insert(CREATE) - {table=dr.test.BRefs, data={brefsText=value1, &dr.test.Trace@crefTraits=Trace(value=traceValue)}, refs={@ref-to-dr.test.CMaster-cref=${ids[0]}, @ref-to-dr.test.CMaster-crefTraits=${ids[1]}, @ref-to-dr.test.CRefDetail-crefDetail=${cInst.all[0].refID.id}}}")
    assert(server.query("dr.test.CRefDetail { * }").toString() == "[{@id=1, crefDetailText=value2}]")
    assert(server.query("dr.test.BRefs { *, crefDetail { * }, cref { * }, crefTraits { * }}").toString() == "[{@id=1, brefsText=value1, &dr.test.Trace@crefTraits=Trace(value=traceValue), crefDetail={@id=1, crefDetailText=value2}, cref={@id=${ids[0]}, cmasterText=value1}, crefTraits={@id=${ids[1]}, cmasterText=value2}}]")

    val uJson1 = """{
      "brefsText":"u-value1",
      "crefDetail":{
        "crefDetailText":"u-value2"
      },
      "cref":{
        "@type":"one-link",
        "ref":${ids[2]}
      },
      "crefTraits":{
        "@type":"one-link-traits",
        "ref":{
          "id":${ids[3]},
          "traits":[{"@type":"dr.test.Trace","value":"traceUpdate"}]
        }
      }
    }"""
    val uInst1 = server.update(BRefs::class, id, uJson1)
    assert(uInst1.all.size == 2)
    assert(uInst1.all[0].toString() == "Insert(ADD) - {table=dr.test.CRefDetail, data={crefDetailText=u-value2}}")
    assert(uInst1.all[1].toString() == "Update(UPDATE) - {table=dr.test.BRefs, id=$id, data={brefsText=u-value1, &dr.test.Trace@crefTraits=Trace(value=traceUpdate)}, refs={@ref-to-dr.test.CMaster-cref=${ids[2]}, @ref-to-dr.test.CMaster-crefTraits=${ids[3]}, @ref-to-dr.test.CRefDetail-crefDetail=${uInst1.all[0].refID.id}}}")
    assert(cInst.all[0].refID.id != uInst1.all[0].refID.id) // confirm that it's a new owned instance and not an update of the old one
    // TODO: @id=1 is orphan. How to handle orphan entities? Probably should update instead of add?
    assert(server.query("dr.test.CRefDetail { * }").toString() == "[{@id=1, crefDetailText=value2}, {@id=2, crefDetailText=u-value2}]")
    assert(server.query("dr.test.BRefs { *, crefDetail { * }, cref { * }, crefTraits { * }}").toString() == "[{@id=1, brefsText=u-value1, &dr.test.Trace@crefTraits=Trace(value=traceUpdate), crefDetail={@id=2, crefDetailText=u-value2}, cref={@id=${ids[2]}, cmasterText=value3}, crefTraits={@id=${ids[3]}, cmasterText=value4}}]")


    // TODO: test unlink of optional refs?
  }

  @Test fun testCollections() {
    val cJson = """{
      "bcolsText":"value1",
      "ccolDetail":[
        {"ccolDetailText":"value2"},
        {"ccolDetailText":"value3"}
      ],
      "ccol":[${ids[0]}, ${ids[1]}],
      "ccolTraits":[
        {"id":${ids[2]},"traits":[{"@type":"dr.test.Trace","value":"trace1"}]},
        {"id":${ids[3]},"traits":[{"@type":"dr.test.Trace","value":"trace2"}]}
      ],
      "ccolUnique":[${ids[0]}, ${ids[1]}]
    }"""
    val cInst = server.create(BCols::class, cJson)
    val id = cInst.root.refID.id!!
    assert(cInst.all.size == 9)
    assert(cInst.all[0].toString() == "Insert(CREATE) - {table=dr.test.BCols, data={bcolsText=value1}}")
    assert(cInst.all[1].toString() == "Insert(ADD) - {table=dr.test.CColDetail, data={ccolDetailText=value2}, refs={@inv-to-dr.test.BCols-ccolDetail=$id}}")
    assert(cInst.all[2].toString() == "Insert(ADD) - {table=dr.test.CColDetail, data={ccolDetailText=value3}, refs={@inv-to-dr.test.BCols-ccolDetail=$id}}")
    assert(cInst.all[3].toString() == "Insert(LINK) - {table=dr.test.BCols-ccol, refs={@ref-to-dr.test.CMaster=${ids[0]}, @inv-to-dr.test.BCols=$id}}")
    assert(cInst.all[4].toString() == "Insert(LINK) - {table=dr.test.BCols-ccol, refs={@ref-to-dr.test.CMaster=${ids[1]}, @inv-to-dr.test.BCols=$id}}")
    assert(cInst.all[5].toString() == "Insert(LINK) - {table=dr.test.BCols-ccolTraits, data={&dr.test.Trace=Trace(value=trace1)}, refs={@ref-to-dr.test.CMaster=${ids[2]}, @inv-to-dr.test.BCols=$id}}")
    assert(cInst.all[6].toString() == "Insert(LINK) - {table=dr.test.BCols-ccolTraits, data={&dr.test.Trace=Trace(value=trace2)}, refs={@ref-to-dr.test.CMaster=${ids[3]}, @inv-to-dr.test.BCols=$id}}")
    assert(cInst.all[7].toString() == "Update(LINK) - {table=dr.test.CMaster, id=${ids[0]}, refs={@inv-to-dr.test.BCols-ccolUnique=$id}}")
    assert(cInst.all[8].toString() == "Update(LINK) - {table=dr.test.CMaster, id=${ids[1]}, refs={@inv-to-dr.test.BCols-ccolUnique=$id}}")
    assert(server.query("dr.test.CColDetail { * }").toString() == "[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}]")
    assert(server.query("dr.test.BCols { *, ccolDetail { * }, ccol { * }, ccolTraits { * }, ccolUnique { * } }").toString() == "[{@id=1, bcolsText=value1, ccolDetail=[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}], ccolUnique=[{@id=${ids[0]}, cmasterText=value1}, {@id=${ids[1]}, cmasterText=value2}], ccol=[{@id=1, ccol={@id=${ids[0]}, cmasterText=value1}}, {@id=2, ccol={@id=${ids[1]}, cmasterText=value2}}], ccolTraits=[{@id=1, &dr.test.Trace=Trace(value=trace1), ccolTraits={@id=${ids[2]}, cmasterText=value3}}, {@id=2, &dr.test.Trace=Trace(value=trace2), ccolTraits={@id=${ids[3]}, cmasterText=value4}}]}]")

    val uJson1 = """{
      "bcolsText":"u-value1",
      "ccolDetail":[
        {"ccolDetailText":"u-value2"},
        {"ccolDetailText":"u-value3"}
      ],
      "ccol":{
        "@type":"many-links",
        "refs":[${ids[3]}, ${ids[2]}]
      },
      "ccolTraits":{
        "@type":"many-links-traits",
        "refs":[
          {"id":${ids[1]},"traits":[{"@type":"dr.test.Trace","value":"u-trace1"}]},
          {"id":${ids[0]},"traits":[{"@type":"dr.test.Trace","value":"u-trace2"}]}
        ]
      },
      "ccolUnique":{
        "@type":"many-links",
        "refs":[${ids[3]}, ${ids[2]}]
      }
    }"""
    val uInst1 = server.update(BCols::class, id, uJson1)
    assert(uInst1.all.size == 9)
    assert(uInst1.all[0].toString() == "Update(UPDATE) - {table=dr.test.BCols, id=1, data={bcolsText=u-value1}}")
    assert(uInst1.all[1].toString() == "Insert(ADD) - {table=dr.test.CColDetail, data={ccolDetailText=u-value2}, refs={@inv-to-dr.test.BCols-ccolDetail=$id}}")
    assert(uInst1.all[2].toString() == "Insert(ADD) - {table=dr.test.CColDetail, data={ccolDetailText=u-value3}, refs={@inv-to-dr.test.BCols-ccolDetail=$id}}")
    assert(uInst1.all[3].toString() == "Insert(LINK) - {table=dr.test.BCols-ccol, refs={@ref-to-dr.test.CMaster=${ids[3]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst1.all[4].toString() == "Insert(LINK) - {table=dr.test.BCols-ccol, refs={@ref-to-dr.test.CMaster=${ids[2]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst1.all[5].toString() == "Insert(LINK) - {table=dr.test.BCols-ccolTraits, data={&dr.test.Trace=Trace(value=u-trace1)}, refs={@ref-to-dr.test.CMaster=${ids[1]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst1.all[6].toString() == "Insert(LINK) - {table=dr.test.BCols-ccolTraits, data={&dr.test.Trace=Trace(value=u-trace2)}, refs={@ref-to-dr.test.CMaster=${ids[0]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst1.all[7].toString() == "Update(LINK) - {table=dr.test.CMaster, id=${ids[3]}, refs={@inv-to-dr.test.BCols-ccolUnique=$id}}")
    assert(uInst1.all[8].toString() == "Update(LINK) - {table=dr.test.CMaster, id=${ids[2]}, refs={@inv-to-dr.test.BCols-ccolUnique=$id}}")
    assert(cInst.all[1].refID.id != uInst1.all[1].refID.id) // confirm that it's a new owned instance and not an update of the old one
    assert(cInst.all[2].refID.id != uInst1.all[2].refID.id) // confirm that it's a new owned instance and not an update of the old one
    // TODO: How to remove from owned collection?
    assert(server.query("dr.test.CColDetail { * }").toString() == "[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}, {@id=3, ccolDetailText=u-value2}, {@id=4, ccolDetailText=u-value3}]")
    assert(server.query("dr.test.BCols { *, ccolDetail { * }, ccol { * }, ccolTraits { * }, ccolUnique { * } }").toString() == "[{@id=1, bcolsText=u-value1, ccolDetail=[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}, {@id=3, ccolDetailText=u-value2}, {@id=4, ccolDetailText=u-value3}], ccolUnique=[{@id=${ids[0]}, cmasterText=value1}, {@id=${ids[1]}, cmasterText=value2}, {@id=${ids[2]}, cmasterText=value3}, {@id=${ids[3]}, cmasterText=value4}], ccol=[{@id=1, ccol={@id=${ids[0]}, cmasterText=value1}}, {@id=2, ccol={@id=${ids[1]}, cmasterText=value2}}, {@id=3, ccol={@id=${ids[3]}, cmasterText=value4}}, {@id=4, ccol={@id=${ids[2]}, cmasterText=value3}}], ccolTraits=[{@id=1, &dr.test.Trace=Trace(value=trace1), ccolTraits={@id=${ids[2]}, cmasterText=value3}}, {@id=2, &dr.test.Trace=Trace(value=trace2), ccolTraits={@id=${ids[3]}, cmasterText=value4}}, {@id=3, &dr.test.Trace=Trace(value=u-trace1), ccolTraits={@id=${ids[1]}, cmasterText=value2}}, {@id=4, &dr.test.Trace=Trace(value=u-trace2), ccolTraits={@id=${ids[0]}, cmasterText=value1}}]}]")

    val uJson2 = """{
      "ccol":{
        "@type":"one-link",
        "ref":${ids[4]}
      },
      "ccolTraits":{
        "@type":"one-link-traits",
        "ref":{"id":${ids[4]},"traits":[{"@type":"dr.test.Trace","value":"u-trace3"}]}
      },
      "ccolUnique":{
        "@type":"one-link",
        "ref":${ids[4]}
      }
    }"""
    val uInst2 = server.update(BCols::class, id, uJson2)
    assert(uInst2.all.size == 4)
    assert(uInst2.all[0].toString() == "Update(UPDATE) - {table=dr.test.BCols, id=$id}")
    assert(uInst2.all[1].toString() == "Insert(LINK) - {table=dr.test.BCols-ccol, refs={@ref-to-dr.test.CMaster=${ids[4]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst2.all[2].toString() == "Insert(LINK) - {table=dr.test.BCols-ccolTraits, data={&dr.test.Trace=Trace(value=u-trace3)}, refs={@ref-to-dr.test.CMaster=${ids[4]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst2.all[3].toString() == "Update(LINK) - {table=dr.test.CMaster, id=${ids[4]}, refs={@inv-to-dr.test.BCols-ccolUnique=$id}}")
    assert(server.query("dr.test.BCols { *, ccolDetail { * }, ccol { * }, ccolTraits { * }, ccolUnique { * } }").toString() == "[{@id=1, bcolsText=u-value1, ccolDetail=[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}, {@id=3, ccolDetailText=u-value2}, {@id=4, ccolDetailText=u-value3}], ccolUnique=[{@id=${ids[0]}, cmasterText=value1}, {@id=${ids[1]}, cmasterText=value2}, {@id=${ids[2]}, cmasterText=value3}, {@id=${ids[3]}, cmasterText=value4}, {@id=${ids[4]}, cmasterText=value5}], ccol=[{@id=1, ccol={@id=${ids[0]}, cmasterText=value1}}, {@id=2, ccol={@id=${ids[1]}, cmasterText=value2}}, {@id=3, ccol={@id=${ids[3]}, cmasterText=value4}}, {@id=4, ccol={@id=${ids[2]}, cmasterText=value3}}, {@id=5, ccol={@id=${ids[4]}, cmasterText=value5}}], ccolTraits=[{@id=1, &dr.test.Trace=Trace(value=trace1), ccolTraits={@id=${ids[2]}, cmasterText=value3}}, {@id=2, &dr.test.Trace=Trace(value=trace2), ccolTraits={@id=${ids[3]}, cmasterText=value4}}, {@id=3, &dr.test.Trace=Trace(value=u-trace1), ccolTraits={@id=${ids[1]}, cmasterText=value2}}, {@id=4, &dr.test.Trace=Trace(value=u-trace2), ccolTraits={@id=${ids[0]}, cmasterText=value1}}, {@id=5, &dr.test.Trace=Trace(value=u-trace3), ccolTraits={@id=${ids[4]}, cmasterText=value5}}]}]")

    val uJson3 = """{
      "ccol":{
        "@type":"one-unlink",
        "ref":${ids[0]}
      },
      "ccolTraits":{
        "@type":"many-unlink",
        "refs":[${ids[0]}, ${ids[1]}]
      },
      "ccolUnique":{
        "@type":"one-unlink",
        "ref":${ids[2]}
      }
    }"""
    val uInst3 = server.update(BCols::class, id, uJson3)
    assert(uInst3.all.size == 5)
    assert(uInst3.all[0].toString() == "Update(UPDATE) - {table=dr.test.BCols, id=$id}")
    assert(uInst3.all[1].toString() == "Delete(UNLINK) - {table=dr.test.BCols-ccol, refs={@ref-to-dr.test.CMaster=${ids[0]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst3.all[2].toString() == "Delete(UNLINK) - {table=dr.test.BCols-ccolTraits, refs={@ref-to-dr.test.CMaster=${ids[0]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst3.all[3].toString() == "Delete(UNLINK) - {table=dr.test.BCols-ccolTraits, refs={@ref-to-dr.test.CMaster=${ids[1]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst3.all[4].toString() == "Update(UNLINK) - {table=dr.test.CMaster, id=${ids[2]}, refs={@inv-to-dr.test.BCols-ccolUnique=null}}")
    assert(server.query("dr.test.BCols { *, ccolDetail { * }, ccol { * }, ccolTraits { * }, ccolUnique { * } }").toString() == "[{@id=1, bcolsText=u-value1, ccolDetail=[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}, {@id=3, ccolDetailText=u-value2}, {@id=4, ccolDetailText=u-value3}], ccolUnique=[{@id=${ids[0]}, cmasterText=value1}, {@id=${ids[1]}, cmasterText=value2}, {@id=${ids[3]}, cmasterText=value4}, {@id=${ids[4]}, cmasterText=value5}], ccol=[{@id=2, ccol={@id=${ids[1]}, cmasterText=value2}}, {@id=3, ccol={@id=${ids[3]}, cmasterText=value4}}, {@id=4, ccol={@id=${ids[2]}, cmasterText=value3}}, {@id=5, ccol={@id=${ids[4]}, cmasterText=value5}}], ccolTraits=[{@id=1, &dr.test.Trace=Trace(value=trace1), ccolTraits={@id=${ids[2]}, cmasterText=value3}}, {@id=2, &dr.test.Trace=Trace(value=trace2), ccolTraits={@id=${ids[3]}, cmasterText=value4}}, {@id=5, &dr.test.Trace=Trace(value=u-trace3), ccolTraits={@id=${ids[4]}, cmasterText=value5}}]}]")
  }

  @Test fun testSealed() {
    val cJsonError = """{"alias":"Alex"}"""
    try { server.create(SuperUser::class, cJsonError) } catch (ex: Exception) {
      assert(ex.message == "A sealed entity must be created via Pack<*>! - (dr.test.SuperUser)")
    }

    val cJson = """{
      "head":{"@type":"dr.test.SuperUser","alias":"Alex"},
      "tail":[{"@type":"dr.test.AdminUser","adminProp":"AlexAdminProp"}]
    }"""
    val cInst = server.create(Pack::class, cJson)
    val id = cInst.root.refID.id!!
    assert(cInst.all.size == 2)
    assert(cInst.all[0].toString() == "Insert(CREATE) - {table=dr.test.SuperUser, data={alias=Alex, @type=dr.test.AdminUser}}")
    assert(cInst.all[1].toString() == "Insert(CREATE) - {table=dr.test.AdminUser, data={adminProp=AlexAdminProp}, refs={@super=$id}}")
    assert(server.query("dr.test.SuperUser { * }").toString() == "[{@id=1, alias=Alex, @type=dr.test.AdminUser}]")
    assert(server.query("dr.test.AdminUser { * }").toString() == "[{@id=1, adminProp=AlexAdminProp}]")


    /*val uJson = """{
      "head":{"@type":"dr.test.SuperUser","alias":"Mario"},
      "tail":[{"@type":"dr.test.AdminUser","adminProp":"MarioAdminProp"}]
    }"""
    val uInst = server.update(Pack::class, id, uJson)
    uInst.all.forEach{ println(it) }*/
  }
}