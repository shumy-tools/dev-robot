package dr.adaptor

import dr.spi.IQueryExecutor
import dr.spi.IResult
import dr.spi.Rows
import org.jooq.Field
import org.jooq.Record
import org.jooq.Select

class QStruct(val main: Select<*>) {
  val inverted = linkedMapOf<String, QStruct>()
}

class MultiQueryExecutor(private val struct: QStruct): IQueryExecutor {
  override fun exec(params: Map<String, Any>): IResult {
    params.forEach { struct.main.bind(it.key, it.value) }

    println(struct.main.sql)

    val data = TData()
    val directResult = struct.main.fetch()
    directResult.forEach { data.process(it) }

    return SQLResult(data)
  }
}

private class SQLResult(val data: TData): IResult {
  override fun <T : Any> get(name: String): T {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
  }

  override fun raw() = data.rows
}


/* ------------------------- helpers -------------------------*/
class TData {
  val rows = mutableListOf<LinkedHashMap<String, Any?>>()

  fun process(record: Record) {
    val row = linkedMapOf<String, Any?>()
    record.fields().forEach {
      val value = it.getValue(record)
      if (it.name.startsWith('.')) row.processJoin(it.name, value) else row[it.name] = value
    }

    rows.add(row)
  }

  @Suppress("UNCHECKED_CAST")
  private fun LinkedHashMap<String, Any?>.processJoin(name: String, value: Any?) {
    val splits = name.split('.').drop(1)

    var place = this
    for (position in splits.dropLast(1)) {
      place = place.getOrPut(position) { linkedMapOf<String, Any?>() } as LinkedHashMap<String, Any?>
    }

    place[splits.last()] = value
  }
}