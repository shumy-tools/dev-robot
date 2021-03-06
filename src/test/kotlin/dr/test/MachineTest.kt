package dr.test

import dr.adaptor.SQLAdaptor
import dr.base.Role
import dr.base.User
import dr.base.loadRoles
import dr.io.Instruction
import dr.schema.SParser
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

  loadRoles()
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

private fun Instruction.checkHistory(user: String, evtType: String?, evt: String?, from: String?, to: String, map: Map<String, Any>) {
  val fields = data.mapKeys { it.key.name }
  assert(table.name == "dr.base.History")
  assert(fields["user"] == user)
  assert(fields["evtType"] == evtType)
  assert(fields["evt"] == evt)
  assert(fields["from"] == from)
  assert(fields["to"] == to)
  assert(fields["data"] == map)
}

@FixMethodOrder
class MachineTest {
  @Test fun testFirstState() {
    val cJson = """{
      "name":"My Name"
    }"""
    val cInst = server.create(MEntity::class, cJson, server.adminUser)
    val id = cInst.root.refID.id!!
    assert(cInst.all.size == 4)
    cInst.all[0].checkHistory(server.adminUser.name,null,null,null,"START", mapOf("d-field" to 30))
    assert(cInst.all[1].toString() == "Insert(CREATE) - {table=dr.test.MEntity, data={@state=START, @open={name={roles={admin=true}}}, name=My Name}}")
    assert(cInst.all[2].toString() == "Insert(LINK) - {table=dr.test.MEntity-history, refs={@ref-to-dr.base.History=${cInst.all[0].refID.id}, @inv-to-dr.test.MEntity=$id}}")
    assert(cInst.all[3].toString() == "Insert(CREATE) - {table=dr.test.RefToMEntity, data={alias=a-create}, refs={@ref-to-dr.test.MEntity-to=$id}}")
    assert(server.query(MEntity::class,"| @id == $id | { *, @history { user, evt, from, to } }").toString() == "[{@id=$id, @state=START, @open={name={roles={admin=true}}}, name=My Name, @history=[{@id=${cInst.all[0].refID.id}, user=shumy, evt=null, from=null, to=START}]}]")
    assert(server.query(RefToMEntity::class,"| @id == ${cInst.all[3].refID.id} | { *, to { @state } }").toString() == "[{@id=${cInst.all[3].refID.id}, alias=a-create, to={@id=$id, @state=START}}]")

    val uJson = """{
      "name":"No Name"
    }"""
    val uInst = server.update(MEntity::class, id, uJson)
    assert(uInst.all.size == 2)
    assert(uInst.all[0].toString() == "Update(UPDATE) - {table=dr.test.MEntity, id=$id, data={name=No Name}}")
    assert(uInst.all[1].toString() == "Insert(CREATE) - {table=dr.test.RefToMEntity, data={alias=a-update}, refs={@ref-to-dr.test.MEntity-to=$id}}")
    assert(server.query(RefToMEntity::class,"| @id == ${uInst.all[1].refID.id} | { *, to { @state } }").toString() == "[{@id=${uInst.all[1].refID.id}, alias=a-update, to={@id=$id, @state=START}}]")
  }

  @Test fun testOkActionFlow() {
    val cJson = """{
      "name":"New Name"
    }"""
    val cInst = server.create(MEntity::class, cJson, server.adminUser)
    val id = cInst.root.refID.id!!
    assert(cInst.all.size == 4)
    cInst.all[0].checkHistory(server.adminUser.name,null,null,null,"START", mapOf("d-field" to 30))
    assert(cInst.all[1].toString() == "Insert(CREATE) - {table=dr.test.MEntity, data={@state=START, @open={name={roles={admin=true}}}, name=New Name}}")
    assert(cInst.all[2].toString() == "Insert(LINK) - {table=dr.test.MEntity-history, refs={@ref-to-dr.base.History=${cInst.all[0].refID.id}, @inv-to-dr.test.MEntity=$id}}")
    assert(cInst.all[3].toString() == "Insert(CREATE) - {table=dr.test.RefToMEntity, data={alias=a-create}, refs={@ref-to-dr.test.MEntity-to=$id}}")
    assert(server.query(MEntity::class,"| @id == $id | { *, @history { user, evt, from, to } }").toString() == "[{@id=$id, @state=START, @open={name={roles={admin=true}}}, name=New Name, @history=[{@id=${cInst.all[0].refID.id}, user=shumy, evt=null, from=null, to=START}]}]")
    assert(server.query(RefToMEntity::class,"| @id == ${cInst.all[3].refID.id} | { *, to { @state } }").toString() == "[{@id=${cInst.all[3].refID.id}, alias=a-create, to={@id=$id, @state=START}}]")

    val eJson1 = """{
      "value":"#try-submit1"
    }"""
    val eInst1 = server.action(MEntity::class, MEntityMachine.Event.Submit::class, id, eJson1, server.adminUser)
    assert(eInst1.all.size == 3)
    eInst1.all[0].checkHistory(server.adminUser.name,"dr.test.MEntityMachine.Event.Submit","""{"value":"#try-submit1"}""","START","VALIDATE", mapOf("owner" to "shumy"))
    assert(eInst1.all[1].toString() == "Update(UPDATE) - {table=dr.test.MEntity, id=$id, data={@state=VALIDATE, @open={}}}")
    assert(eInst1.all[2].toString() == "Insert(LINK) - {table=dr.test.MEntity-history, refs={@ref-to-dr.base.History=${eInst1.all[0].refID.id}, @inv-to-dr.test.MEntity=$id}}")
    assert(server.query(MEntity::class,"| @id == $id | { @state, @open, @history { user, evt, from, to, data } }").toString() == """[{@id=$id, @state=VALIDATE, @open={}, @history=[{@id=${cInst.all[0].refID.id}, user=shumy, evt=null, from=null, to=START, data={d-field=30}}, {@id=${eInst1.all[0].refID.id}, user=shumy, evt={"value":"#try-submit1"}, from=START, to=VALIDATE, data={owner=shumy}}]}]""")

    val eInst2 = server.action(MEntity::class, MEntityMachine.Event.Ok::class, id, "{}", server.managerUser)
    assert(eInst2.all.size == 4)
    assert(eInst2.all[0].toString() == "Insert(CREATE) - {table=dr.test.RefToMEntity, data={alias=a-stop}, refs={@ref-to-dr.test.MEntity-to=$id}}")
    eInst2.all[1].checkHistory(server.managerUser.name,"dr.test.MEntityMachine.Event.Ok","{}","VALIDATE","STOP", mapOf())
    assert(eInst2.all[2].toString() == "Update(UPDATE) - {table=dr.test.MEntity, id=$id, data={@state=STOP, @open={}}}")
    assert(eInst2.all[3].toString() == "Insert(LINK) - {table=dr.test.MEntity-history, refs={@ref-to-dr.base.History=${eInst2.all[1].refID.id}, @inv-to-dr.test.MEntity=$id}}")
    assert(server.query(MEntity::class,"| @id == $id | { @state, @open, @history { user, evt, from, to, data } }").toString() == """[{@id=$id, @state=STOP, @open={}, @history=[{@id=${cInst.all[0].refID.id}, user=shumy, evt=null, from=null, to=START, data={d-field=30}}, {@id=${eInst1.all[0].refID.id}, user=shumy, evt={"value":"#try-submit1"}, from=START, to=VALIDATE, data={owner=shumy}}, {@id=${eInst2.all[1].refID.id}, user=alex, evt={}, from=VALIDATE, to=STOP, data={}}]}]""")
  }

  @Test fun testIncorrectActionFlow() {
    val cJson = """{
      "name":"Other Name"
    }"""
    val cInst = server.create(MEntity::class, cJson, server.adminUser)
    val id = cInst.root.refID.id!!
    assert(cInst.all.size == 4)
    cInst.all[0].checkHistory(server.adminUser.name,null,null,null,"START", mapOf("d-field" to 30))
    assert(cInst.all[1].toString() == "Insert(CREATE) - {table=dr.test.MEntity, data={@state=START, @open={name={roles={admin=true}}}, name=Other Name}}")
    assert(cInst.all[2].toString() == "Insert(LINK) - {table=dr.test.MEntity-history, refs={@ref-to-dr.base.History=${cInst.all[0].refID.id}, @inv-to-dr.test.MEntity=$id}}")
    assert(cInst.all[3].toString() == "Insert(CREATE) - {table=dr.test.RefToMEntity, data={alias=a-create}, refs={@ref-to-dr.test.MEntity-to=$id}}")
    assert(server.query(MEntity::class,"| @id == $id | { *, @history { user, evt, from, to } }").toString() == "[{@id=$id, @state=START, @open={name={roles={admin=true}}}, name=Other Name, @history=[{@id=${cInst.all[0].refID.id}, user=shumy, evt=null, from=null, to=START}]}]")
    assert(server.query(RefToMEntity::class,"| @id == ${cInst.all[3].refID.id} | { *, to { @state } }").toString() == "[{@id=${cInst.all[3].refID.id}, alias=a-create, to={@id=$id, @state=START}}]")

    val eJson1 = """{
      "value":"#try-submit2"
    }"""
    val eInst1 = server.action(MEntity::class, MEntityMachine.Event.Submit::class, id, eJson1, server.adminUser)
    assert(eInst1.all.size == 3)
    eInst1.all[0].checkHistory(server.adminUser.name,"dr.test.MEntityMachine.Event.Submit","""{"value":"#try-submit2"}""","START","VALIDATE", mapOf("owner" to "shumy"))
    assert(eInst1.all[1].toString() == "Update(UPDATE) - {table=dr.test.MEntity, id=$id, data={@state=VALIDATE, @open={}}}")
    assert(eInst1.all[2].toString() == "Insert(LINK) - {table=dr.test.MEntity-history, refs={@ref-to-dr.base.History=${eInst1.all[0].refID.id}, @inv-to-dr.test.MEntity=$id}}")
    assert(server.query(MEntity::class,"| @id == $id | { @state, @open, @history { user, evt, from, to, data } }").toString() == """[{@id=$id, @state=VALIDATE, @open={}, @history=[{@id=${cInst.all[0].refID.id}, user=shumy, evt=null, from=null, to=START, data={d-field=30}}, {@id=${eInst1.all[0].refID.id}, user=shumy, evt={"value":"#try-submit2"}, from=START, to=VALIDATE, data={owner=shumy}}]}]""")

    val eInst2 = server.action(MEntity::class, MEntityMachine.Event.Incorrect::class, id, "{}", server.managerUser)
    assert(eInst2.all.size == 3)
    eInst2.all[0].checkHistory(server.managerUser.name,"dr.test.MEntityMachine.Event.Incorrect","{}","VALIDATE","START", mapOf("d-field" to 30))
    assert(eInst2.all[1].toString() == "Update(UPDATE) - {table=dr.test.MEntity, id=$id, data={@state=START, @open={name={users={shumy=true}}}}}")
    assert(eInst2.all[2].toString() == "Insert(LINK) - {table=dr.test.MEntity-history, refs={@ref-to-dr.base.History=${eInst2.all[0].refID.id}, @inv-to-dr.test.MEntity=$id}}")
    assert(server.query(MEntity::class,"| @id == $id | { @state, @open, @history { user, evt, from, to, data } }").toString() == """[{@id=$id, @state=START, @open={name={users={shumy=true}}}, @history=[{@id=${cInst.all[0].refID.id}, user=shumy, evt=null, from=null, to=START, data={d-field=30}}, {@id=${eInst1.all[0].refID.id}, user=shumy, evt={"value":"#try-submit2"}, from=START, to=VALIDATE, data={owner=shumy}}, {@id=${eInst2.all[0].refID.id}, user=alex, evt={}, from=VALIDATE, to=START, data={d-field=30}}]}]""")
  }
}