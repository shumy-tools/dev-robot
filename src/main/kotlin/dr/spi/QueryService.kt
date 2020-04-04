package dr.spi

import dr.query.QTree

interface IAccessed {
  fun getEntityName(): String
  fun getPaths(): Map<String, Any>
}

interface IResult {
  fun toJson(): String
}

interface IQueryExecutor {
  fun exec(params: Map<String, Any>): IResult
}

interface IQueryAdaptor {
  fun compile(query: QTree): IQueryExecutor
}

interface IQueryAuthorizer {
  fun authorize(accessed: IAccessed): Boolean
}