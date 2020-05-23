package dr.spi

import dr.io.Instructions
import dr.query.QTree
import dr.schema.tabular.Tables
import kotlin.reflect.KProperty

interface IAdaptor {
  val tables: Tables
  fun commit(instructions: Instructions)
  fun compile(query: QTree): IQueryExecutor
}

interface IQueryExecutor {
  fun exec(params: Map<String, Any>): IResult
  fun exec(vararg params: Pair<String, Any>) = exec(params.toMap())
}

typealias QRow = Map<String, Any?>

interface IResult {
  val rows: List<QRow>
  fun <R: Any> get(name: String): R?
  fun <R: Any> get(prop: KProperty<R>): R? = get<R>(prop.name)
}