package dr.adaptor

import dr.query.*
import dr.schema.tabular.ID
import dr.schema.tabular.Table
import dr.schema.tabular.Tables
import dr.spi.IQueryExecutor
import dr.spi.IResult
import org.jooq.*
import org.jooq.impl.DSL

/*if (!sRelation.isCollection) {
  if (sRelation.type == RelationType.CREATE && (sRelation.isUnique || sRelation.ref.type == EntityType.TRAIT)) {
    // A {<rel>: <fields>}
    // filter-each ("@<rel>_<field>" in ?)
  } else {
    // A ref_<rel> --> B
    // match ("@ref__<rel>", "@id" in ?)
  }
} else {
  if (sRelation.type == RelationType.CREATE || (sRelation.type == RelationType.LINK && sRelation.isUnique && sRelation.traits.isEmpty())) {
    // A <-- inv_<A>_<rel> B
    // match ("@id", "@inv_<A>__<rel>" in ?)
  } else {
    // A <-- [inv ref] --> B
    // match ("@id", JOIN, "@id" in ?)
  }
}*/

class SQLQueryExecutor(private val db: DSLContext, private val tables: Tables, query: QTree): IQueryExecutor {
  private val main: Select<*>
  private val inverted = linkedMapOf<String, SQLQueryExecutor>()

  // compile QTree
  init {
    val mTable = tables.get(query.entity)
    main = query.run { mTable.select(select, filter, limit, page) }
  }

  override fun exec(params: Map<String, Any>): IResult {
    println(main.sql)
    params.forEach { main.bind(it.key, it.value) }

    val data = TData()
    val directResult = main.fetch()
    directResult.forEach { data.process(it) }

    return SQLResult(data)
  }

  private fun Table.select(selection: QSelect, filter: QExpression?, limit: Int?, page: Int?): Select<*> {
    val mTable = DSL.table(sqlName()).asTable(MAIN)
    val allFields = joinFields(selection)

    val dbSelect = db.select(allFields).from(mTable)
    val dbJoin = joinTables(dbSelect, selection)
    val dbFiltered = if (filter != null) dbJoin.where(expression(filter)) else dbJoin.where()

    return when {
      page != null -> dbFiltered.limit(limit).offset((page-1) * limit!!)
      limit != null -> dbFiltered.limit(limit)
      else -> dbFiltered
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun Table.joinFields(selection: QSelect, prefix: String = MAIN, alias: String = "", joinFields: MutableList<Field<Any>> = mutableListOf()): List<Field<Any>> {
    val idField = if (prefix == MAIN) idFn(prefix) else idFn(prefix).`as`("$alias.$ID")
    joinFields.add(idField as Field<Any>)

    val fields = if (prefix == MAIN) dbFields(selection, prefix) else dbFields(selection, prefix).map { it.`as`("$alias.${it.name}") }
    joinFields.addAll(fields)

    val refs = dbDirectRefs(selection)
    for (qRel in refs.keys) {
      val rTable = tables.get(qRel.ref)
      rTable.joinFields(qRel.select, qRel.name, "$alias.${qRel.name}", joinFields)
    }

    return joinFields
  }

  private fun Table.joinTables(dbSelect: SelectJoinStep<*>, selection: QSelect, prefix: String = MAIN): SelectJoinStep<*> {
    var nextJoin = dbSelect

    val refs = dbDirectRefs(selection)
    for (dRef in refs.values) {
      val rTable = DSL.table(dRef.rel.ref.sqlName()).asTable(dRef.rel.name)
      val rField = dRef.fn(this, prefix)
      val rId = idFn(dRef.rel.name)
      nextJoin = dbSelect.join(rTable).on(rField.eq(rId))
    }

    for (qRel in refs.keys) {
      val nextTable = tables.get(qRel.ref)
      nextJoin = nextTable.joinTables(nextJoin, qRel.select, qRel.name)
    }

    return nextJoin
  }

  private fun Table.expression(expr: QExpression, prefix: String = MAIN): Condition = if (expr.predicate != null) {
    filter(expr.predicate)
  } else {
    val left = expression(expr.left!!, prefix)
    val right = expression(expr.right!!, prefix)

    when(expr.oper!!) {
      OperType.OR -> left.or(right)
      OperType.AND -> left.and(right)
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun filter(predicate: QPredicate, prefix: String = MAIN): Condition {
    // TODO: build a sub-query path!
    val subQuery = predicate.path.first().name

    //mapper.params[subQuery] = predicate.param

    val path = DSL.field(DSL.name(prefix, subQuery))
    val value = if (predicate.param.type == ParamType.PARAM) DSL.param(predicate.param.value as String) else predicate.param.value

    return when(predicate.comp) {
      CompType.EQUAL -> path.eq(value)
      CompType.DIFFERENT -> path.ne(value)
      CompType.MORE -> path.greaterThan(value)
      CompType.LESS -> path.lessThan(value)
      CompType.MORE_EQ -> path.greaterOrEqual(value)
      CompType.LESS_EQ -> path.lessOrEqual(value)
      CompType.IN -> path.`in`(value)
    }
  }
}


/* ------------------------- helpers -------------------------*/
private class SQLResult(val data: TData): IResult {
  override fun <T : Any> get(name: String): T {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
  }

  override fun raw() = data.rows
}

private class TData {
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