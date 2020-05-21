package dr.test

import dr.DrServer
import dr.adaptor.SQLAdaptor
import dr.schema.Pack
import dr.schema.SParser
import dr.schema.Traits
import org.junit.Test

private val schema = SParser.parse(Country::class, User::class, Role::class, SuperUser::class)
private val adaptor = SQLAdaptor(schema, "jdbc:h2:mem:QueryTest").also {
  it.createSchema()
}

private val server = DrServer(schema, adaptor, TestAuthorizer()).also {
  it.use {
    val portugalID = create(Country("Portugal"))
    val franceID = create(Country("France"))
    val germanyID = create(Country("Germany"))

    val adminID = create(Role("admin", 1))
    val operID = create(Role("oper", 2))
    val otherID = create(Role("other", 3))

    create(User("Alex", "mail@pt", Address(portugalID, "Lisboa", null),
      listOf(
        Setting("Alex-set1", "Alex-v1"),
        Setting("Alex-set2", "Alex-v2")
      ),
      listOf(
        Traits(adminID, Trace("Alex-admin-trace"))
      )
    ))

    create(User("Pedro", "mail@pt", Address(portugalID, "Aveiro", "Gloria"),
      listOf(
        Setting("Pedro-set1", "Pedro-v1"),
        Setting("Pedro-set2", "Pedro-v2"),
        Setting("Pedro-set3", "Pedro-v3")
      ),
      listOf(
        Traits(adminID, Trace("Pedro-admin-trace")),
        Traits(operID, Trace("Pedro-oper-trace")),
        Traits(otherID, Trace("Pedro-other-trace"))
      )
    ))

    create(User("Maria", "mail@com", Address(franceID, "Paris", null),
      listOf(
        Setting("Maria-set1", "Maria-v1")
      ),
      listOf(
        Traits(operID, Trace("Maria-oper-trait"))
      )
    ))

    create(User("Jose", "mail@pt", Address(portugalID, "Aveiro", "Aradas"),
      listOf(
        Setting("Jose-set1", "Jose-v1"),
        Setting("Jose-set2", "Jose-v2")
      ),
      listOf(
        Traits(otherID, Trace("Jose-oper-trait"))
      )
    ))

    create(User("Arnaldo", "mail@com", Address(germanyID, "Berlin", null),
      listOf(
        Setting("Arnaldo-set1", "Arnaldo-v1"),
        Setting("Arnaldo-set2", "Arnaldo-v2"),
        Setting("Arnaldo-set3", "Arnaldo-v3"),
        Setting("Arnaldo-set4", "Arnaldo-v4")
      ),
      listOf(
        Traits(adminID, Trace("Arnaldo-admin-trait"))
      )
    ))

    create(Pack(SuperUser("Bruno"), AdminUser("BrunoAdminProp")))
    create(Pack(SuperUser("Mario"), OperUser("MarioOperProp")))
    create(Pack(SuperUser("Alberto"), OperUser("AlbertoOperProp")))
  }
}

class QueryTest {
  @Test fun testSimpleQuery() {
    server.use {
      val query = query("""dr.test.User | name == "Alex" and email == ?mail | {
        *
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{*={}}")
      }
      val res = query.exec("mail" to "mail@pt")
      assert(res.rows.toString() == "[{@id=1, name=Alex, email=mail@pt, timestamp=1918-01-10T12:35:18}]")
    }
  }

  @Test fun testSortByQuery() {
    server.use {
      val query1 = query("""dr.test.User {
        (asc 1) name
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}}")
      }
      val res1 = query1.exec()
      assert(res1.rows.toString() == "[{@id=1, name=Alex}, {@id=5, name=Arnaldo}, {@id=4, name=Jose}, {@id=3, name=Maria}, {@id=2, name=Pedro}]")

      val res2 = query("""dr.test.User {
        (dsc 1) name
      }""").exec()
      assert(res2.rows.toString() == "[{@id=2, name=Pedro}, {@id=3, name=Maria}, {@id=4, name=Jose}, {@id=5, name=Arnaldo}, {@id=1, name=Alex}]")
    }
  }

  @Test fun tesLimitAndPageQuery() {
    server.use {
      val query1 = query("""dr.test.User | email == "mail@pt" | limit 2 page 1 {
        name
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}, email={}}")
      }
      val res1 = query1.exec()
      assert(res1.rows.toString() == "[{@id=1, name=Alex}, {@id=2, name=Pedro}]")

      val res2 = query("""dr.test.User limit 2 page 2 {
        name
      }""").exec()
      assert(res2.rows.toString() == "[{@id=3, name=Maria}, {@id=4, name=Jose}]")

      val res3 = query("""dr.test.User limit ?lmt page ?pgt {
        name
      }""").exec("lmt" to 2, "pgt" to 3)
      assert(res3.rows.toString() == "[{@id=5, name=Arnaldo}]")
    }
  }

  @Test fun testSimpleInQuery() {
    server.use {
      val query1 = query("""dr.test.User | name in ?names | {
        name, email
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}, email={}}")
      }
      val res1 = query1.exec("names" to listOf("Alex", "Arnaldo"))
      assert(res1.rows.toString() == "[{@id=1, name=Alex, email=mail@pt}, {@id=5, name=Arnaldo, email=mail@com}]")

      val res2 = query("""dr.test.User | name in ["Alex", "Arnaldo"] | {
        name, email
      }""").exec()
      assert(res2.rows.toString() == "[{@id=1, name=Alex, email=mail@pt}, {@id=5, name=Arnaldo, email=mail@com}]")
    }
  }

  @Test fun testOneToOneQuery() {
    server.use {
      val query1 = query("""dr.test.User | address.location == ?loc | {
        name, 
        address { * }
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}, address={*={}}}")
      }
      val res1 = query1.exec("loc" to "Aradas")
      assert(res1.rows.toString() == "[{@id=4, name=Jose, address={@id=4, city=Aveiro, location=Aradas}}]")

      val query2 = query("""dr.test.User | email == "mail@pt" and address.location == ?loc and address.country.name == "Portugal" | {
        name, 
        address { city, country { * } }
      }""")  {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}, address={city={}, country={*={}}, location={}}, email={}}")
      }
      val res2 = query2.exec("loc" to "Aradas")
      assert(res2.rows.toString() == "[{@id=4, name=Jose, address={@id=4, city=Aveiro, country={@id=1, name=Portugal}}}]")
    }
  }

  @Test fun testOneToManyQuery() {
    server.use {
      val query1 = query("""dr.test.User | address.location == "Aradas" | {
        name, 
        settings { * }
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}, settings={*={}}, address={location={}}}")
      }
      val res1 = query1.exec()
      assert(res1.rows.toString() == "[{@id=4, name=Jose, settings=[{@id=7, key=Jose-set1, value=Jose-v1}, {@id=8, key=Jose-set2, value=Jose-v2}]}]")

      val query2 = query("""dr.test.User | address.location == "Aradas" | {
        name, 
        settings | key == "Jose-set2" | { value }
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}, settings={value={}, key={}}, address={location={}}}")
      }
      val res2 = query2.exec()
      assert(res2.rows.toString() == "[{@id=4, name=Jose, settings=[{@id=8, value=Jose-v2}]}]")

      val query3 = query("""dr.test.User | settings.value == "Jose-v2" | {
        name
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}, settings={value={}}}")
      }
      val res3 = query3.exec()
      assert(res3.rows.toString() == "[{@id=4, name=Jose}]")

      val res4 = query("""dr.test.User | settings.value in ["Jose-v2", "Arnaldo-v2"] | {
        name
      }""").exec()
      assert(res4.rows.toString() == "[{@id=4, name=Jose}, {@id=5, name=Arnaldo}]")

      val res5 = query("""dr.test.User | settings.value in ?values | {
        name
      }""").exec("values" to listOf("Jose-v2", "Arnaldo-v2"))
      assert(res5.rows.toString() == "[{@id=4, name=Jose}, {@id=5, name=Arnaldo}]")
    }
  }

  @Test fun testManyToManyQuery() {
    server.use {
      val query1 = query("""dr.test.User | address.location == "Gloria" | {
        name,
        roles { name }
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}, roles={name={}}, address={location={}}}")
      }
      val res1 = query1.exec()
      assert(res1.rows.toString() == "[{@id=2, name=Pedro, roles=[{@id=2, roles={@id=1, name=admin}}, {@id=3, roles={@id=2, name=oper}}, {@id=4, roles={@id=3, name=other}}]}]")

      val query2 = query("""dr.test.User | address.location == "Gloria" | {
        name,
        roles | name == "admin" | { name }
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}, roles={name={}}, address={location={}}}")
      }
      val res2 = query2.exec()
      assert(res2.rows.toString() == "[{@id=2, name=Pedro, roles=[{@id=2, roles={@id=1, name=admin}}]}]")

      val query3 = query("""dr.test.User | address.location == "Aradas" | {
        name, 
        roles { &dr.test.Trace, name, ord }
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}, roles={&dr.test.Trace={}, name={}, ord={}}, address={location={}}}")
      }
      val res3 = query3.exec()
      assert(res3.rows.toString() == "[{@id=4, name=Jose, roles=[{@id=6, &dr.test.Trace=Trace(value=Jose-oper-trait), roles={@id=3, name=other, ord=3}}]}]")

      val query4 = query("""dr.test.User | roles.name == "admin" | {
        name
      }""") {
        assert(it.entity == "dr.test.User")
        assert(it.paths.toString() == "{name={}, roles={name={}}}")
      }
      val res4 = query4.exec()
      assert(res4.rows.toString() == "[{@id=1, name=Alex}, {@id=2, name=Pedro}, {@id=5, name=Arnaldo}]")

      val res5 = query("""dr.test.User | roles.name in ["admin", "oper"] | {
        name
      }""").exec()
      assert(res5.rows.toString() == "[{@id=1, name=Alex}, {@id=2, name=Pedro}, {@id=3, name=Maria}, {@id=5, name=Arnaldo}]")

      val res6 = query("""dr.test.User | roles.name in ?values | {
        name
      }""").exec("values" to listOf("admin", "oper"))
      assert(res6.rows.toString() == "[{@id=1, name=Alex}, {@id=2, name=Pedro}, {@id=3, name=Maria}, {@id=5, name=Arnaldo}]")
    }
  }

  @Test fun testSealedQuery() {
    server.use {
      val query1 = query("""dr.test.SuperUser | alias == "Mario" | {
        alias, @type
      }""") {
        assert(it.entity == "dr.test.SuperUser")
        assert(it.paths.toString() == "{alias={}, @type={}}")
      }
      val res1 = query1.exec()
      assert(res1.rows.toString() == "[{@id=2, alias=Mario, @type=dr.test.OperUser}]")

      val query2 = query("""dr.test.AdminUser {
        adminProp, @super { * }
      }""") {
        assert(it.entity == "dr.test.AdminUser")
        assert(it.paths.toString() == "{adminProp={}, @super={*={}}}")
      }
      val res2 = query2.exec()
      assert(res2.rows.toString() == "[{@id=1, adminProp=BrunoAdminProp, @super={@id=1, alias=Bruno, @type=dr.test.AdminUser}}]")

      val query3 = query("""dr.test.OperUser | @super.alias == "Alberto" | {
        operProp
      }""") {
        assert(it.entity == "dr.test.OperUser")
        assert(it.paths.toString() == "{operProp={}, @super={alias={}}}")
      }
      val res3 = query3.exec()
      assert(res3.rows.toString() == "[{@id=2, operProp=AlbertoOperProp}]")
    }
  }
}