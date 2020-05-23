package dr.test

import dr.adaptor.SQLAdaptor
import dr.base.Role
import dr.base.User
import dr.io.Instruction
import dr.schema.SParser
import dr.schema.tabular.HISTORY
import dr.schema.tabular.STATE
import dr.spi.QRow
import org.junit.FixMethodOrder
import org.junit.Test

private val schema = SParser.parse(RefToMEntity::class, MEntity::class)
private val adaptor = SQLAdaptor(schema, "jdbc:h2:mem:StateMachineTest").also {
  it.createSchema()
}

private val server = TestServer(schema, adaptor)

private fun createRoles(): List<Long> {
  val cJson1 = """{
    "name":"admin"
  }"""
  val cInst1 = server.create(Role::class, cJson1)
  val id1 = cInst1.root.refID.id!!

  val cJson2 = """{
    "name":"manager"
  }"""
  val cInst2 = server.create(Role::class, cJson2)
  val id2 = cInst2.root.refID.id!!

  return listOf(id1, id2)
}

private fun createUsers(): List<Long> {
  val cJson1 = """{
    "name":"shumy",
    "email":"shumy@gmail.com",
    "roles":[${roles[0]}]
  }"""
  val cInst1 = server.create(User::class, cJson1)
  val id1 = cInst1.root.refID.id!!

  val cJson2 = """{
    "name":"alex",
    "email":"alex@gmail.com",
    "roles":[${roles[1]}]
  }"""
  val cInst2 = server.create(User::class, cJson2)
  val id2 = cInst2.root.refID.id!!

  return listOf(id1, id2)
}

private val roles = createRoles()
private val users = createUsers()

private fun Instruction.checkHistory(evtType: String?, evt: String?, from: String?, to: String, hData: String) {
  val fields = data.mapKeys { it.key.name }
  assert(table.name == "dr.base.History")
  assert(fields["evtType"] == evtType)
  assert(fields["evt"] == evt)
  assert(fields["from"] == from)
  assert(fields["to"] == to)
  assert(fields["data"] == hData)
}

@FixMethodOrder
class MachineTest {
  @Test fun testFirstState() {
    val cJson = """{
      "name":"My Name"
    }"""
    val cInst = server.create(MEntity::class, cJson)
    val id = cInst.root.refID.id!!
    assert(cInst.all.size == 4)
    cInst.all[0].checkHistory(null,null,null,"START","""{"d-field":30}""")
    assert(cInst.all[1].toString() == "Insert(CREATE) - {table=dr.test.MEntity, data={@state=START, name=My Name}}")
    assert(cInst.all[2].toString() == "Insert(LINK) - {table=dr.test.MEntity-history, refs={@ref-to-dr.base.History=1, @inv-to-dr.test.MEntity=1}}")
    assert(cInst.all[3].toString() == "Insert(CREATE) - {table=dr.test.RefToMEntity, data={alias=a-create}, refs={@ref-to-dr.test.MEntity-to=$id}}")
    assert(server.query("dr.test.MEntity { *, @history { evt, from, to } }").toString() == "[{@id=$id, @state=START, name=My Name, @history=[{@id=1, evt=null, from=null, to=START}]}]")
    assert(server.query("dr.test.RefToMEntity { *, to { * } }").toString() == "[{@id=1, alias=a-create, to={@id=$id, @state=START, name=My Name}}]")

    val uJson = """{
      "name":"No Name"
    }"""
    val uInst = server.update(MEntity::class, id, uJson)
    assert(uInst.all.size == 2)
    assert(uInst.all[0].toString() == "Update(UPDATE) - {table=dr.test.MEntity, id=$id, data={name=No Name}}")
    assert(uInst.all[1].toString() == "Insert(CREATE) - {table=dr.test.RefToMEntity, data={alias=a-update}, refs={@ref-to-dr.test.MEntity-to=$id}}")
    assert(server.query("dr.test.RefToMEntity { *, to { * } }").toString() == "[{@id=1, alias=a-create, to={@id=$id, @state=START, name=No Name}}, {@id=2, alias=a-update, to={@id=$id, @state=START, name=No Name}}]")
  }

  @Test fun testAction() {
    val eJson1 = """{
      "value":"#try-submit"
    }"""
    val eInst1 = server.action(MEntity::class, MEntityMachine.Event.Submit::class, 1, eJson1)
    assert(eInst1.all.size == 3)
    eInst1.all[0].checkHistory("dr.test.MEntityMachine.Event.Submit","""{"value":"#try-submit"}""","START","VALIDATE","""{"owner":"anonymous"}""")
    assert(eInst1.all[1].toString() == "Update(UPDATE) - {table=dr.test.MEntity, id=1, data={@state=VALIDATE}}")
    assert(eInst1.all[2].toString() == "Insert(LINK) - {table=dr.test.MEntity-history, refs={@ref-to-dr.base.History=2, @inv-to-dr.test.MEntity=1}}")
    assert(server.query("dr.test.MEntity | @id == 1 | { @state, @history { evt, from, to, data } }").toString() == """[{@id=1, @state=VALIDATE, @history=[{@id=1, evt=null, from=null, to=START, data={"d-field":30}}, {@id=2, evt={"value":"#try-submit"}, from=START, to=VALIDATE, data={"owner":"anonymous"}}]}]""")

    val eInst2 = server.action(MEntity::class, MEntityMachine.Event.Ok::class, 1, "{}")
    eInst2.all.forEach { println(it) }
    assert(eInst2.all.size == 4)
    assert(eInst2.all[0].toString() == "Insert(CREATE) - {table=dr.test.RefToMEntity, data={alias=a-stop}, refs={@ref-to-dr.test.MEntity-to=1}}")
    eInst2.all[1].checkHistory("dr.test.MEntityMachine.Event.Ok","{}","VALIDATE","STOP","{}")
    assert(eInst2.all[2].toString() == "Update(UPDATE) - {table=dr.test.MEntity, id=1, data={@state=STOP}}")
    assert(eInst2.all[3].toString() == "Insert(LINK) - {table=dr.test.MEntity-history, refs={@ref-to-dr.base.History=3, @inv-to-dr.test.MEntity=1}}")
    assert(server.query("dr.test.MEntity | @id == 1 | { @state, @history { evt, from, to, data } }").toString() == """[{@id=1, @state=STOP, @history=[{@id=1, evt=null, from=null, to=START, data={"d-field":30}}, {@id=2, evt={"value":"#try-submit"}, from=START, to=VALIDATE, data={"owner":"anonymous"}}, {@id=3, evt={}, from=VALIDATE, to=STOP, data={}}]}]""")

    //println(server.query("dr.test.MEntity | @id == 1 | { @state, @history { evt, from, to, data } }"))
  }
}