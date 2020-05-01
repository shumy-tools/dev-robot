package dr.spi

import dr.query.QTree
import dr.io.Instructions
import dr.schema.tabular.Tables

interface IAdaptor {
  fun tables(): Tables
  fun commit(instructions: Instructions)
  fun compile(query: QTree): IQueryExecutor
}

interface IQueryExecutor {
  fun exec(params: Map<String, Any>): IResult
  fun exec(vararg params: Pair<String, Any>) = exec(params.toMap())
}

typealias QRow = Map<String, Any?>

interface IResult {
  fun <T: Any> get(name: String): T

  fun row(id: Long): QRow?
  fun rows(): List<QRow>
}