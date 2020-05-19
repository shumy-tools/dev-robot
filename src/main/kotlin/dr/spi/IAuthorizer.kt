package dr.spi

interface IAuthorizer {
  fun read(access: IReadAccess): Boolean
}

interface IReadAccess {
  val entity: String
  val paths: Map<String, Any>
}