package dr.adaptor

import dr.spi.IQueryExecutor
import dr.spi.IResult
import dr.spi.Rows
import org.jooq.Select

class MultiQueryExecutor(private val struct: QStruct): IQueryExecutor {
  override fun exec(params: Map<String, Any>): IResult {
    params.forEach { struct.main.bind(it.key, it.value) }

    println(struct.main.sql)
    println(struct.main.fetch())
    return SQLResult(emptyList())
  }
}

data class QStruct(val main: Select<*>) {
  val inverted = linkedMapOf<String, QStruct>()
}

private class SQLResult(val raw: Rows): IResult {
  override fun <T : Any> get(name: String): T {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
  }

  override fun raw() = raw
}
