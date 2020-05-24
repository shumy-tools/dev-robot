package dr.spi

import dr.io.Instructions
import dr.query.QTree
import dr.schema.tabular.Tables
import kotlin.reflect.KProperty1

interface IAdaptor {
  val tables: Tables
  fun commit(instructions: Instructions)
  fun compile(query: QTree): IQueryExecutor<Any>
}

interface IQueryExecutor<E: Any> {
  fun exec(params: Map<String, Any>): IResult<E>
  fun exec(vararg params: Pair<String, Any>) = exec(params.toMap())
}

typealias QRow = Map<String, Any?>

interface IRowGet<E: Any> {
  fun <R: Any> get(name: String): R?
  fun <R: Any> get(prop: KProperty1<E, R>): R? = get<R>(prop.name)
}

interface IResult<E: Any>: IRowGet<E>, Iterable<IRowGet<E>> {
  val rows: List<QRow>
  fun isEmpty(): Boolean = rows.isEmpty()
}