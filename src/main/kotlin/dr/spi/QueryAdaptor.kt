package dr.spi

import dr.query.QTree

interface IQueryAdaptor {
  fun compile(query: QTree): IQueryExecutor
}

interface IQueryExecutor {
  fun exec(params: Map<String, Any>): IResult
}

  interface IResult {
    fun toJson(): String
  }

interface IQueryAuthorizer {
  fun authorize(accessed: IAccessed): Boolean
}

  interface IAccessed {
    fun getEntityName(): String
    fun getPaths(): Map<String, Any>
  }
