package dr

import dr.adaptor.SQLAdaptor
import dr.schema.SParser
import dr.schema.tabular.TParser
import dr.spi.IAuthorizer
import dr.spi.IReadAccess

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

  val server = DrServer(schema, adaptor, TestAuthorizer()).also {
    it.start(8080)
  }

  /*
  println("Q1")
  DrServer.qEngine.compile("""dr.User | name == "Mica*" | limit 10 {
    (asc 1) name
  }""".trimIndent())

  println("Q2")
  DrServer.qEngine.compile("""dr.User | name == "Mica*" | limit 10 page 2 { * }""".trimIndent())

  println("Q3")
  DrServer.qEngine.compile("""dr.User | name == "Mica*" | {
    name, email,
    address {
      country, city
    },
    roles | name == "admin" and order == 10 | { * }
  }""".trimMargin())

  println("Q4")
  DrServer.qEngine.compile("""dr.User | name == "Mica*" and roles..order == 1 | { * }""")

  println("Q5")
  val query = DrServer.qEngine.compile("""dr.User |  email == "email" and (name == "Mica*" or roles..name == ?name) | { * }""")
  query.exec(mapOf("name" to "admin"))

  println("")
  val userType = UserType("shumy", "mail@google.pt", "pass-1", setOf(1L, 2L))
  val customer = Customer(12F, EmbeddedAddress("France", "Paris"))

  val ownedUserType = OwnedUserType("Top Boss", listOf(Pack(userType, customer))).apply { inputOrDerived = "input-value" }
  val jsonOwnedUserTypeCreate = server.serialize(ownedUserType)

  val customerId = server.mEngine.create(server.deserialize(jsonOwnedUserTypeCreate, OwnedUserType::class))
  println("OWNED-USER-TYPE-ID: $customerId")


  println("")
  val jsonSellCreate = server.serialize(Sell(10.5F, Pair(100L, Traits(EmbeddedAddress("France", "Paris")))))
  val sellId = server.mEngine.create(server.deserialize(jsonSellCreate, Sell::class))
  println("SELL-ID: $sellId")


  println("")
  val jsonUserCreate = server.serialize(User(
    name = "Micael",
    email = "email@gmail.com",
    market = Pair(1L, Traits(UserMarket(Trace(LocalDateTime.now()),1L))),
    address = Address("Portugal", "Aveiro", "Some street and number"),
    //roles = mapOf(1L to Traits(Trace(LocalDateTime.now())), 2L to Traits(Trace(LocalDateTime.now())))
    roles = listOf(1L, 2L)
  ))
  val userId = server.mEngine.create(server.deserialize(jsonUserCreate, User::class))
  println("USER-ID: $userId")


  println("")
  val jsonUserUpdate = server.serialize(UpdateData(
    "name" to "Micael",
    "email" to "email@gmail.com",
    "market" to OneLinkWithTraits(5L, Traits(UserMarket(Trace(LocalDateTime.of(2000, Month.JANUARY, 1, 12, 0,0)),2L)))
  ))
  server.mEngine.update(User::class.qualifiedName!!, userId, server.deserialize(jsonUserUpdate, UpdateData::class))

  println("")
  val jsonAuctionCreate = server.serialize(Auction(
    name = "Continente",
    items = setOf(1L, 2L, 3L),
    bids = listOf(
      Bid(price = 10F, boxes = 10, from = userId, item = 1L, detail = BidDetail("detail-1")),
      Bid(price = 13F, boxes = 5, from = userId, item = 2L, detail = BidDetail("detail-2"))
    )
  ))
  val auctionId = server.mEngine.create(server.deserialize(jsonAuctionCreate, Auction::class))
  println("AUCTION-ID: $auctionId")


  println("")
  val jsonBidAdd = server.serialize(Bid(price = 23F, boxes = 1, from = userId, item = 3L, detail = BidDetail("detail-3")))
  val bidId = server.mEngine.add(Auction::class.qualifiedName!!, auctionId, "bids", server.deserialize(jsonBidAdd, Bid::class))
  println("BID-ID: $bidId")


  println("")
  //val jsonRolesLink = DrServer.serialize(ManyLinksWithTraits(6L to Traits(Trace(LocalDateTime.now())), 7L to Traits(Trace(LocalDateTime.now()))))
  //val roleIds = DrServer.mEngine.link(User::class.qualifiedName!!, userId, "roles", DrServer.deserialize(jsonRolesLink, ManyLinksWithTraits::class))
  val roleIds = server.mEngine.link(User::class.qualifiedName!!, userId, "roles", ManyLinksWithoutTraits(6L, 7L))
  println("ROLE-ID: $roleIds")


  println("")
  val jsonRolesUnlink = server.serialize(ManyLinkDelete(roleIds))
  server.mEngine.unlink(User::class.qualifiedName!!, "roles", server.deserialize(jsonRolesUnlink, ManyLinkDelete::class))

  println("OWNED-USER-TYPE-CREATE: $jsonOwnedUserTypeCreate\n")
  println("SELL-CREATE: $jsonSellCreate\n")
  println("USER-CREATE: $jsonUserCreate\n")
  println("USER-UPDATE: $jsonUserUpdate\n")
  println("AUCTION-CREATE: $jsonAuctionCreate\n")
  println("BID-ADD: $jsonBidAdd\n")
  println("ROLES-LINK: $jsonRolesLink\n")
  println("ROLES-UNLINK: $jsonRolesUnlink\n")*/
}
