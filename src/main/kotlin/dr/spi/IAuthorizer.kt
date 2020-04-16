package dr.spi

interface IAuthorizer {
  fun read(access: IReadAccess): Boolean
}

interface IReadAccess {
  fun entity(): String
  fun paths(): Map<String, Any>
}