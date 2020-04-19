package dr.spi

import dr.query.QTree

interface IQueryAdaptor {
  fun compile(query: QTree): IQueryExecutor
}

interface IQueryExecutor {
  fun accessed(): IReadAccess
  fun exec(params: Map<String, Any>): IResult
}

  interface IResult {
    fun toJson(): String
  }
