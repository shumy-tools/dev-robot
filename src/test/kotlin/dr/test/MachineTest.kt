package dr.test

import dr.adaptor.SQLAdaptor
import dr.base.Role
import dr.base.User
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

@FixMethodOrder
class MachineTest {
  @Test fun testFirstState() {
    val cJson = """{
      "name":"My Name"
    }"""
    val cInst = server.create(MEntity::class, cJson)
    cInst.all.forEach { println(it) }
    val id = cInst.root.refID.id!!
    assert(cInst.all.size == 4)
    //assert(cInst.all[0].toString() == "Insert(CREATE) - {table=dr.base.History, data={ts=2020-05-22T23:03:55.379, state=START}}")
    assert(cInst.all[1].toString() == "Insert(CREATE) - {table=dr.test.MEntity, data={@state=START, name=My Name}}")
    assert(cInst.all[2].toString() == "Insert(LINK) - {table=dr.test.MEntity-history, refs={@ref-to-dr.base.History=1, @inv-to-dr.test.MEntity=1}}")
    assert(cInst.all[3].toString() == "Insert(CREATE) - {table=dr.test.RefToMEntity, data={alias=m-alias}, refs={@ref-to-dr.test.MEntity-to=$id}}")

    //println(server.query("dr.test.RefToMEntity { *, to { * } }"))
    assert(server.query("dr.test.MEntity { *, @history { state } }").toString() == "[{@id=$id, @state=START, name=My Name, @history=[{@id=1, state=START}]}]")
    assert(server.query("dr.test.RefToMEntity { *, to { * } }").toString() == "[{@id=1, alias=m-alias, to={@id=$id, @state=START, name=My Name}}]")
  }
}