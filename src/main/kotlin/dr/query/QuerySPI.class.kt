package dr.query

class CompiledQuery(val query: QueryParser.QueryContext) {
  override fun toString(): String {
    return this.query.text
  }
}

interface QueryExecutor {
  fun exec()
}

interface QueryAdaptor {
  fun compile(query: CompiledQuery): QueryExecutor
}