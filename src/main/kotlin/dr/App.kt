package dr

import dr.adaptor.SQLAdaptor
import dr.io.JsonParser
import dr.schema.SParser
import dr.schema.Traits
import dr.schema.tabular.ID
import dr.schema.tabular.TParser
import dr.spi.IAuthorizer
import dr.spi.IReadAccess
import java.time.LocalDateTime

class TestAuthorizer: IAuthorizer {
  override fun read(access: IReadAccess): Boolean {
    println("access = $access")
    return true
  }
}

fun main(args: Array<String>) {
  val schema = SParser.parse(OwnedUserType::class, Sell::class, User::class, Role::class, Auction::class)
  val adaptor = SQLAdaptor(schema, "jdbc:h2:mem:testdb").also {
    it.createSchema()
  }

  DrServer(schema, adaptor, TestAuthorizer()).also {
    it.start(8080)
    it.use {
      val adminID = create(Role("admin", 1))
      val operID = create(Role("oper", 2))
      val otherID = create(Role("other", 3))

      create(User("Alex", "mail@pt", listOf(Address("Portugal", "Lisboa", null), Address("Portugal", "Porto", "alternative")), listOf(
        Traits(adminID, Trace(LocalDateTime.now()))
      )))

      create(User("Pedro", "mail@pt", listOf(Address("Portugal", "Aveiro", null)), listOf(
        Traits(adminID, Trace(LocalDateTime.now())),
        Traits(operID, Trace(LocalDateTime.now())),
        Traits(otherID, Trace(LocalDateTime.now()))
      )))

      create(User("Maria", "mail@com", listOf(Address("France", "Paris", "a-detail")), listOf(
        Traits(operID, Trace(LocalDateTime.now()))
      )))

      create(User("Jose", "mail@pt", listOf(Address("Portugal", "Aveiro", null)), listOf(
        Traits(otherID, Trace(LocalDateTime.now()))
      )))

      create(User("Arnaldo", "mail@pt", listOf(Address("Germany", "Berlin", null)), listOf(
        Traits(adminID, Trace(LocalDateTime.now()))
      )))
    }
  }
}
