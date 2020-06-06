package dr.test

import dr.adaptor.SQLAdaptor
import dr.schema.Pack
import dr.schema.SParser
import org.junit.FixMethodOrder
import org.junit.Test

private val schema = SParser.parse(A::class, BRefs::class, BCols::class, CMaster::class, SuperUser::class, RefsToPack::class, Tree::class)
private val adaptor = SQLAdaptor(schema, "jdbc:h2:mem:InputTest").also {
  it.createSchema()
}

private val server = TestServer(schema, adaptor)

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
    assert(server.query(CMaster::class,"{ * }").toString() == "[{@id=1, cmasterText=value1}, {@id=2, cmasterText=value2}, {@id=3, cmasterText=value3}, {@id=4, cmasterText=value4}, {@id=5, cmasterText=value5}]")
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
    assert(server.query(A::class,"{ * }").toString() == "[{@id=1, oneText=one, twoInt=2, threeLong=3, fourFloat=4.0, fiveDouble=5.0, sixBoolean=true, sevenTime=10:30:20, eightDate=2020-01-25, nineDateTime=2020-01-25T10:30:20, timestamp=1918-01-10T12:35:18}]")

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
    assert(server.query(A::class,"{ * }").toString() == "[{@id=1, oneText=u-one, twoInt=20, threeLong=30, fourFloat=40.0, fiveDouble=50.0, sixBoolean=false, sevenTime=10:40:25, eightDate=2019-02-12, nineDateTime=2018-03-10T10:45:35, timestamp=1918-01-10T12:35:18}]")
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
    assert(cInst.all[1].toString() == "Insert(CREATE) - {table=dr.test.BRefs, data={brefsText=value1, &dr.test.Trace@crefTraits=Trace(value=traceValue)}, refs={@ref-to-dr.test.CRefDetail-crefDetail=${cInst.all[0].refID.id}, @ref-to-dr.test.CMaster-cref=${ids[0]}, @ref-to-dr.test.CMaster-crefTraits=${ids[1]}}}")
    assert(server.query(CRefDetail::class,"{ * }").toString() == "[{@id=1, crefDetailText=value2}]")
    assert(server.query(BRefs::class,"{ *, crefDetail { * }, cref { * }, crefTraits { * }}").toString() == "[{@id=1, brefsText=value1, &dr.test.Trace@crefTraits=Trace(value=traceValue), crefDetail={@id=1, crefDetailText=value2}, cref={@id=${ids[0]}, cmasterText=value1}, crefTraits={@id=${ids[1]}, cmasterText=value2}}]")

    // TODO: @id=1 is orphan. How to handle orphan entities? Mark as orphan?
    val uJson1 = """{
      "brefsText":"u-value1",
      "crefDetail":{
        "@type":"one-add",
        "value":{"crefDetailText":"u-value2"}
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
    assert(uInst1.all[1].toString() == "Update(UPDATE) - {table=dr.test.BRefs, id=$id, data={brefsText=u-value1, &dr.test.Trace@crefTraits=Trace(value=traceUpdate)}, refs={@ref-to-dr.test.CRefDetail-crefDetail=${uInst1.all[0].refID.id}, @ref-to-dr.test.CMaster-cref=${ids[2]}, @ref-to-dr.test.CMaster-crefTraits=${ids[3]}}}")
    assert(cInst.all[0].refID.id != uInst1.all[0].refID.id) // confirm that it's a new owned instance and not an update of the old one
    assert(server.query(CRefDetail::class,"{ * }").toString() == "[{@id=1, crefDetailText=value2}, {@id=2, crefDetailText=u-value2}]")
    assert(server.query(BRefs::class,"{ *, crefDetail { * }, cref { * }, crefTraits { * }}").toString() == "[{@id=1, brefsText=u-value1, &dr.test.Trace@crefTraits=Trace(value=traceUpdate), crefDetail={@id=2, crefDetailText=u-value2}, cref={@id=${ids[2]}, cmasterText=value3}, crefTraits={@id=${ids[3]}, cmasterText=value4}}]")

    val uJson2 = """{
      "crefDetail":{
        "@type":"one-rmv"
      },
      "cref":{
        "@type":"one-unlink"
      },
      "crefTraits":{
        "@type":"one-unlink"
      }
    }"""
    val uInst2 = server.update(BRefs::class, id, uJson2)
    assert(uInst2.all.size == 1)
    assert(uInst2.all[0].toString() == "Update(UPDATE) - {table=dr.test.BRefs, id=$id, data={&dr.test.Trace@crefTraits=null}, refs={@ref-to-dr.test.CRefDetail-crefDetail=null, @ref-to-dr.test.CMaster-cref=null, @ref-to-dr.test.CMaster-crefTraits=null}}")
    assert(server.query(BRefs::class,"{ *, crefDetail { * }, cref { * }, crefTraits { * }}").toString() == "[{@id=1, brefsText=u-value1, &dr.test.Trace@crefTraits=null, crefDetail={@id=null, crefDetailText=null}, cref={@id=null, cmasterText=null}, crefTraits={@id=null, cmasterText=null}}]")

    val cJsonNulls = """{
      "brefsText":null,
      "crefDetail":null,
      "cref":null,
      "crefTraits":null
    }"""
    val cInstNulls = server.create(BRefs::class, cJsonNulls)
    assert(cInstNulls.all.size == 1)
    assert(cInstNulls.all[0].toString() == "Insert(CREATE) - {table=dr.test.BRefs, data={brefsText=null, &dr.test.Trace@crefTraits=null}, refs={@ref-to-dr.test.CRefDetail-crefDetail=null, @ref-to-dr.test.CMaster-cref=null, @ref-to-dr.test.CMaster-crefTraits=null}}")
    assert(server.query(BRefs::class,"| @id == 2 | { *, crefDetail { * }, cref { * }, crefTraits { * }}").toString() == "[{@id=2, brefsText=null, &dr.test.Trace@crefTraits=null, crefDetail={@id=null, crefDetailText=null}, cref={@id=null, cmasterText=null}, crefTraits={@id=null, cmasterText=null}}]")
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
    assert(server.query(CColDetail::class,"{ * }").toString() == "[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}]")
    assert(server.query(BCols::class,"{ *, ccolDetail { * }, ccol { * }, ccolTraits { * }, ccolUnique { * } }").toString() == "[{@id=1, bcolsText=value1, ccolDetail=[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}], ccolUnique=[{@id=${ids[0]}, cmasterText=value1}, {@id=${ids[1]}, cmasterText=value2}], ccol=[{@id=${ids[0]}, cmasterText=value1}, {@id=${ids[1]}, cmasterText=value2}], ccolTraits=[{&dr.test.Trace=Trace(value=trace1), @id=${ids[2]}, cmasterText=value3}, {&dr.test.Trace=Trace(value=trace2), @id=${ids[3]}, cmasterText=value4}]}]")

    val uJson1 = """{
      "bcolsText":"u-value1",
      "ccolDetail": {
        "@type":"many-add",
        "values":[
          {"ccolDetailText":"u-value2"},
          {"ccolDetailText":"u-value3"}
        ]
      },
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
    assert(server.query(CColDetail::class,"{ * }").toString() == "[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}, {@id=3, ccolDetailText=u-value2}, {@id=4, ccolDetailText=u-value3}]")
    assert(server.query(BCols::class,"{ *, ccolDetail { * }, ccol { * }, ccolTraits { * }, ccolUnique { * } }").toString() == "[{@id=1, bcolsText=u-value1, ccolDetail=[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}, {@id=3, ccolDetailText=u-value2}, {@id=4, ccolDetailText=u-value3}], ccolUnique=[{@id=${ids[0]}, cmasterText=value1}, {@id=${ids[1]}, cmasterText=value2}, {@id=${ids[2]}, cmasterText=value3}, {@id=${ids[3]}, cmasterText=value4}], ccol=[{@id=${ids[0]}, cmasterText=value1}, {@id=${ids[1]}, cmasterText=value2}, {@id=${ids[3]}, cmasterText=value4}, {@id=${ids[2]}, cmasterText=value3}], ccolTraits=[{&dr.test.Trace=Trace(value=trace1), @id=${ids[2]}, cmasterText=value3}, {&dr.test.Trace=Trace(value=trace2), @id=${ids[3]}, cmasterText=value4}, {&dr.test.Trace=Trace(value=u-trace1), @id=${ids[1]}, cmasterText=value2}, {&dr.test.Trace=Trace(value=u-trace2), @id=${ids[0]}, cmasterText=value1}]}]")

    val uJson2 = """{
      "ccolDetail": {
        "@type":"one-add",
        "value":{"ccolDetailText":"u-value4"}
      },
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
    assert(uInst2.all.size == 5)
    assert(uInst2.all[0].toString() == "Update(UPDATE) - {table=dr.test.BCols, id=$id}")
    assert(uInst2.all[1].toString() == "Insert(ADD) - {table=dr.test.CColDetail, data={ccolDetailText=u-value4}, refs={@inv-to-dr.test.BCols-ccolDetail=$id}}")
    assert(uInst2.all[2].toString() == "Insert(LINK) - {table=dr.test.BCols-ccol, refs={@ref-to-dr.test.CMaster=${ids[4]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst2.all[3].toString() == "Insert(LINK) - {table=dr.test.BCols-ccolTraits, data={&dr.test.Trace=Trace(value=u-trace3)}, refs={@ref-to-dr.test.CMaster=${ids[4]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst2.all[4].toString() == "Update(LINK) - {table=dr.test.CMaster, id=${ids[4]}, refs={@inv-to-dr.test.BCols-ccolUnique=$id}}")
    assert(server.query(BCols::class,"{ *, ccolDetail { * }, ccol { * }, ccolTraits { * }, ccolUnique { * } }").toString() == "[{@id=1, bcolsText=u-value1, ccolDetail=[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}, {@id=3, ccolDetailText=u-value2}, {@id=4, ccolDetailText=u-value3}, {@id=5, ccolDetailText=u-value4}], ccolUnique=[{@id=${ids[0]}, cmasterText=value1}, {@id=${ids[1]}, cmasterText=value2}, {@id=${ids[2]}, cmasterText=value3}, {@id=${ids[3]}, cmasterText=value4}, {@id=${ids[4]}, cmasterText=value5}], ccol=[{@id=${ids[0]}, cmasterText=value1}, {@id=${ids[1]}, cmasterText=value2}, {@id=${ids[3]}, cmasterText=value4}, {@id=${ids[2]}, cmasterText=value3}, {@id=${ids[4]}, cmasterText=value5}], ccolTraits=[{&dr.test.Trace=Trace(value=trace1), @id=${ids[2]}, cmasterText=value3}, {&dr.test.Trace=Trace(value=trace2), @id=${ids[3]}, cmasterText=value4}, {&dr.test.Trace=Trace(value=u-trace1), @id=${ids[1]}, cmasterText=value2}, {&dr.test.Trace=Trace(value=u-trace2), @id=${ids[0]}, cmasterText=value1}, {&dr.test.Trace=Trace(value=u-trace3), @id=${ids[4]}, cmasterText=value5}]}]")

    val uJson3 = """{
      "ccolDetail":{
        "@type":"one-rmv",
        "ref":${uInst1.all[1].refID.id}
      },
      "ccol":{
        "@type":"one-unlink",
        "ref":${ids[0]}
      },
      "ccolTraits":{
        "@type":"one-unlink",
        "ref":${ids[0]}
      },
      "ccolUnique":{
        "@type":"one-unlink",
        "ref":${ids[2]}
      }
    }"""
    val uInst3 = server.update(BCols::class, id, uJson3)
    assert(uInst3.all.size == 5)
    assert(uInst3.all[0].toString() == "Update(UPDATE) - {table=dr.test.BCols, id=$id}")
    assert(uInst3.all[1].toString() == "Update(REMOVE) - {table=dr.test.CColDetail, id=${uInst1.all[1].refID.id}, refs={@inv-to-dr.test.BCols-ccolDetail=null}}")
    assert(uInst3.all[2].toString() == "Delete(UNLINK) - {table=dr.test.BCols-ccol, refs={@ref-to-dr.test.CMaster=${ids[0]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst3.all[3].toString() == "Delete(UNLINK) - {table=dr.test.BCols-ccolTraits, refs={@ref-to-dr.test.CMaster=${ids[0]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst3.all[4].toString() == "Update(UNLINK) - {table=dr.test.CMaster, id=${ids[2]}, refs={@inv-to-dr.test.BCols-ccolUnique=null}}")
    assert(server.query(BCols::class,"{ *, ccolDetail { * }, ccol { * }, ccolTraits { * }, ccolUnique { * } }").toString() == "[{@id=1, bcolsText=u-value1, ccolDetail=[{@id=1, ccolDetailText=value2}, {@id=2, ccolDetailText=value3}, {@id=4, ccolDetailText=u-value3}, {@id=5, ccolDetailText=u-value4}], ccolUnique=[{@id=${ids[0]}, cmasterText=value1}, {@id=${ids[1]}, cmasterText=value2}, {@id=${ids[3]}, cmasterText=value4}, {@id=${ids[4]}, cmasterText=value5}], ccol=[{@id=${ids[1]}, cmasterText=value2}, {@id=${ids[3]}, cmasterText=value4}, {@id=${ids[2]}, cmasterText=value3}, {@id=${ids[4]}, cmasterText=value5}], ccolTraits=[{&dr.test.Trace=Trace(value=trace1), @id=${ids[2]}, cmasterText=value3}, {&dr.test.Trace=Trace(value=trace2), @id=${ids[3]}, cmasterText=value4}, {&dr.test.Trace=Trace(value=u-trace1), @id=${ids[1]}, cmasterText=value2}, {&dr.test.Trace=Trace(value=u-trace3), @id=${ids[4]}, cmasterText=value5}]}]")

    val uJson4 = """{
      "ccolDetail": {
        "@type":"many-rmv",
        "refs":[${cInst.all[1].refID.id}, ${cInst.all[2].refID.id}]
      },
      "ccol":{
        "@type":"many-unlink",
        "refs":[${ids[1]}, ${ids[2]}]
      },
      "ccolTraits":{
        "@type":"many-unlink",
        "refs":[${ids[1]}, ${ids[2]}]
      },
      "ccolUnique":{
        "@type":"many-unlink",
        "refs":[${ids[0]}, ${ids[1]}]
      }
    }"""
    val uInst4 = server.update(BCols::class, id, uJson4)
    assert(uInst4.all.size == 9)
    assert(uInst4.all[0].toString() == "Update(UPDATE) - {table=dr.test.BCols, id=$id}")
    assert(uInst4.all[1].toString() == "Update(REMOVE) - {table=dr.test.CColDetail, id=${cInst.all[1].refID.id}, refs={@inv-to-dr.test.BCols-ccolDetail=null}}")
    assert(uInst4.all[2].toString() == "Update(REMOVE) - {table=dr.test.CColDetail, id=${cInst.all[2].refID.id}, refs={@inv-to-dr.test.BCols-ccolDetail=null}}")
    assert(uInst4.all[3].toString() == "Delete(UNLINK) - {table=dr.test.BCols-ccol, refs={@ref-to-dr.test.CMaster=${ids[1]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst4.all[4].toString() == "Delete(UNLINK) - {table=dr.test.BCols-ccol, refs={@ref-to-dr.test.CMaster=${ids[2]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst4.all[5].toString() == "Delete(UNLINK) - {table=dr.test.BCols-ccolTraits, refs={@ref-to-dr.test.CMaster=${ids[1]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst4.all[6].toString() == "Delete(UNLINK) - {table=dr.test.BCols-ccolTraits, refs={@ref-to-dr.test.CMaster=${ids[2]}, @inv-to-dr.test.BCols=$id}}")
    assert(uInst4.all[7].toString() == "Update(UNLINK) - {table=dr.test.CMaster, id=${ids[0]}, refs={@inv-to-dr.test.BCols-ccolUnique=null}}")
    assert(uInst4.all[8].toString() == "Update(UNLINK) - {table=dr.test.CMaster, id=${ids[1]}, refs={@inv-to-dr.test.BCols-ccolUnique=null}}")
    assert(server.query(BCols::class,"{ *, ccolDetail { * }, ccol { * }, ccolTraits { * }, ccolUnique { * } }").toString() == "[{@id=1, bcolsText=u-value1, ccolDetail=[{@id=4, ccolDetailText=u-value3}, {@id=5, ccolDetailText=u-value4}], ccolUnique=[{@id=${ids[3]}, cmasterText=value4}, {@id=${ids[4]}, cmasterText=value5}], ccol=[{@id=${ids[3]}, cmasterText=value4}, {@id=${ids[4]}, cmasterText=value5}], ccolTraits=[{&dr.test.Trace=Trace(value=trace2), @id=${ids[3]}, cmasterText=value4}, {&dr.test.Trace=Trace(value=u-trace3), @id=${ids[4]}, cmasterText=value5}]}]")
  }

  @Test fun testSealed() {
    val cJsonError = """{"alias":"Alex"}"""
    try { server.create(SuperUser::class, cJsonError) } catch (ex: Exception) {
      assert(ex.message == "A sealed entity must be created via Pack<*>! - (dr.test.SuperUser)")
    }

    val cJson1 = """{
      "head":{"@type":"dr.test.SuperUser","alias":"Alex"},
      "tail":[{"@type":"dr.test.AdminUser","adminProp":"AlexAdminProp"}]
    }"""
    val cInst1 = server.create(Pack::class, cJson1)
    val id1 = cInst1.root.refID.id!!
    assert(cInst1.all.size == 2)
    assert(cInst1.all[0].toString() == "Insert(CREATE) - {table=dr.test.SuperUser, data={alias=Alex, @type=dr.test.AdminUser}}")
    assert(cInst1.all[1].toString() == "Insert(CREATE) - {table=dr.test.AdminUser, data={adminProp=AlexAdminProp}, refs={@super=$id1}}")
    assert(server.query(AdminUser::class,"{ *, @super { * }}").toString() == "[{@id=1, adminProp=AlexAdminProp, @super={@id=1, alias=Alex, @type=dr.test.AdminUser}}]")

    val cJson2 = """{
      "head":{"@type":"dr.test.SuperUser","alias":"Mario"},
      "tail":[{"@type":"dr.test.OperUser","operProp":"MarioOperProp"}]
    }"""
    val cInst2 = server.create(Pack::class, cJson2)
    val id2 = cInst2.root.refID.id!!
    assert(cInst2.all.size == 2)
    assert(cInst2.all[0].toString() == "Insert(CREATE) - {table=dr.test.SuperUser, data={alias=Mario, @type=dr.test.OperUser}}")
    assert(cInst2.all[1].toString() == "Insert(CREATE) - {table=dr.test.OperUser, data={operProp=MarioOperProp}, refs={@super=$id2}}")
    assert(server.query(OperUser::class,"{ *, @super { * }}").toString() == "[{@id=1, operProp=MarioOperProp, @super={@id=2, alias=Mario, @type=dr.test.OperUser}}]")

    assert(server.query(SuperUser::class,"{ * }").toString() == "[{@id=1, alias=Alex, @type=dr.test.AdminUser}, {@id=2, alias=Mario, @type=dr.test.OperUser}]")

    val cJson3 = """{
      "ownedAdmin":{
        "head":{"@type":"dr.test.OwnedSuperUser","ownedAlias":"Pedro"},
        "tail":[{"@type":"dr.test.OwnedOperUser","ownedOperProp":"PedroOperProp"}]
      },
      "admin":$id1,
      "oper":${cInst2.all[1].refID.id}
    }"""
    val cInst3 = server.create(RefsToPack::class, cJson3)
    val id3 = cInst3.root.refID.id!!
    assert(cInst3.all.size == 3)
    assert(cInst3.all[0].toString() == "Insert(ADD) - {table=dr.test.OwnedSuperUser, data={ownedAlias=Pedro, @type=dr.test.OwnedOperUser}}")
    assert(cInst3.all[1].toString() == "Insert(CREATE) - {table=dr.test.OwnedOperUser, data={ownedOperProp=PedroOperProp}, refs={@super=1}}")
    assert(cInst3.all[2].toString() == "Insert(CREATE) - {table=dr.test.RefsToPack, refs={@ref-to-dr.test.OwnedSuperUser-ownedAdmin=${cInst3.all[0].refID.id}, @ref-to-dr.test.SuperUser-admin=$id1, @ref-to-dr.test.OperUser-oper=${cInst2.all[1].refID.id}}}")
    assert(server.query(RefsToPack::class,"{ @id, ownedAdmin { * }, admin { alias }, oper { operProp, @super { alias } }}").toString() == "[{@id=1, ownedAdmin={@id=1, ownedAlias=Pedro, @type=dr.test.OwnedOperUser}, admin={@id=$id1, alias=Alex}, oper={@id=1, operProp=MarioOperProp, @super={@id=$id2, alias=Mario}}}]")

    val cJson4 = """{
      "ownedAdmin":null,
      "admin":$id1,
      "oper":${cInst2.all[1].refID.id}
    }"""
    val cInst4 = server.create(RefsToPack::class, cJson4)
    val id4 = cInst4.root.refID.id!!
    assert(cInst4.all.size == 1)
    assert(cInst4.all[0].toString() == "Insert(CREATE) - {table=dr.test.RefsToPack, refs={@ref-to-dr.test.OwnedSuperUser-ownedAdmin=null, @ref-to-dr.test.SuperUser-admin=$id1, @ref-to-dr.test.OperUser-oper=${cInst2.all[1].refID.id}}}")
    assert(server.query(RefsToPack::class,"| @id == $id4 | { @id, ownedAdmin { * } }").toString() == "[{@id=2, ownedAdmin={@id=null, ownedAlias=null, @type=null}}]")

    val uJson = """{
      "ownedAdmin":{
        "@type":"one-rmv"
      }
    }"""
    val uInst = server.update(RefsToPack::class, id3, uJson)
    assert(uInst.all.size == 1)
    assert(uInst.all[0].toString() == "Update(UPDATE) - {table=dr.test.RefsToPack, id=$id3, refs={@ref-to-dr.test.OwnedSuperUser-ownedAdmin=null}}")
    assert(server.query(RefsToPack::class,"| @id == $id3 | { @id, ownedAdmin { * } }").toString() == "[{@id=$id3, ownedAdmin={@id=null, ownedAlias=null, @type=null}}]")
  }

  @Test fun testNestedTree() {
    val cJson1 = """{
      "treeName":"the-tree",
      "node1":[
        {
          "n1Name":"node-1",
          "node2":[
            {"n2Name":"node-1-1"},
            {"n2Name":"node-1-2"}
          ]
        },
        {
          "n1Name":"node-2",
          "node2":[
            {"n2Name":"node-2-1"},
            {"n2Name":"node-2-2"}
          ]
        }
      ]
    }"""
    val cInst1 = server.create(Tree::class, cJson1)
    val id1 = cInst1.root.refID.id!!
    assert(cInst1.all.size == 7)
    assert(cInst1.all[0].toString() == "Insert(CREATE) - {table=dr.test.Tree, data={treeName=the-tree}}")
    assert(cInst1.all[1].toString() == "Insert(ADD) - {table=dr.test.Node1, data={n1Name=node-1}, refs={@inv-to-dr.test.Tree-node1=$id1}}")
    assert(cInst1.all[2].toString() == "Insert(ADD) - {table=dr.test.Node2, data={n2Name=node-1-1}, refs={@inv-to-dr.test.Node1-node2=${cInst1.all[1].refID.id}}}")
    assert(cInst1.all[3].toString() == "Insert(ADD) - {table=dr.test.Node2, data={n2Name=node-1-2}, refs={@inv-to-dr.test.Node1-node2=${cInst1.all[1].refID.id}}}")
    assert(cInst1.all[4].toString() == "Insert(ADD) - {table=dr.test.Node1, data={n1Name=node-2}, refs={@inv-to-dr.test.Tree-node1=$id1}}")
    assert(cInst1.all[5].toString() == "Insert(ADD) - {table=dr.test.Node2, data={n2Name=node-2-1}, refs={@inv-to-dr.test.Node1-node2=${cInst1.all[4].refID.id}}}")
    assert(cInst1.all[6].toString() == "Insert(ADD) - {table=dr.test.Node2, data={n2Name=node-2-2}, refs={@inv-to-dr.test.Node1-node2=${cInst1.all[4].refID.id}}}")

    val uJson1 = """{
      "node1":{
        "@type":"one-add",
        "value":{
          "n1Name":"node-3",
          "node2":[
            {"n2Name":"node-3-1"},
            {"n2Name":"node-3-2"}
          ]
        }
      }
    }"""
    val uInst1 = server.update(Tree::class, id1, uJson1)
    val id2 = uInst1.all[1].refID.id!!
    assert(uInst1.all.size == 4)
    assert(uInst1.all[0].toString() == "Update(UPDATE) - {table=dr.test.Tree, id=$id1}")
    assert(uInst1.all[1].toString() == "Insert(ADD) - {table=dr.test.Node1, data={n1Name=node-3}, refs={@inv-to-dr.test.Tree-node1=$id1}}")
    assert(uInst1.all[2].toString() == "Insert(ADD) - {table=dr.test.Node2, data={n2Name=node-3-1}, refs={@inv-to-dr.test.Node1-node2=$id2}}")
    assert(uInst1.all[3].toString() == "Insert(ADD) - {table=dr.test.Node2, data={n2Name=node-3-2}, refs={@inv-to-dr.test.Node1-node2=$id2}}")

    val uJson2 = """{
      "node2":{
        "@type":"one-add",
        "value":{"n2Name":"node-3-3"}
      }
    }"""
    val uInst2 = server.update(Node1::class, id2, uJson2)
    assert(uInst2.all.size == 2)
    assert(uInst2.all[0].toString() == "Update(UPDATE) - {table=dr.test.Node1, id=$id2}")
    assert(uInst2.all[1].toString() == "Insert(ADD) - {table=dr.test.Node2, data={n2Name=node-3-3}, refs={@inv-to-dr.test.Node1-node2=$id2}}")
    println(server.query(Tree::class,"{ *, node1 | @id == $id2 | { *, node2 { * }}}").toString())
    assert(server.query(Tree::class,"{ *, node1 | @id == $id2 | { *, node2 { * }}}").toString() == "[{@id=$id1, treeName=the-tree, node1=[{@id=$id2, n1Name=node-3, node2=[{@id=5, n2Name=node-3-1}, {@id=6, n2Name=node-3-2}, {@id=7, n2Name=node-3-3}]}]}]")
  }
}