package dr.spi

import dr.query.QTree
import dr.schema.SEntity

interface IAccessed {
  fun getEntityName(): String
  fun getPaths(): Map<String, Any>
}

interface IQueryExecutor {
  fun exec()
}

interface IQueryAdaptor {
  fun compile(query: QTree): IQueryExecutor
}

interface IQueryAuthorize {
  fun authorized(accessed: IAccessed): Boolean
}