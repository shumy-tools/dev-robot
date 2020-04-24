package dr.spi

import dr.query.QTree

interface IQueryAdaptor {
  fun compile(query: QTree): IQueryExecutor
}

interface IQueryExecutor {
  fun accessed(): IReadAccess
  fun exec(params: Map<String, Any>): Map<String, Any>

  fun exec(vararg params: Pair<String, Any>) = exec(params.toMap())
}

