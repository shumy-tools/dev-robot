package dr.adaptor

import dr.query.*
import dr.schema.tabular.ID
import dr.schema.tabular.STable
import dr.schema.tabular.Tables
import dr.spi.IQueryExecutor
import dr.spi.IResult
import org.jooq.*
import org.jooq.impl.DSL.*

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

class SQLQueryExecutor(private val db: DSLContext, private val tables: Tables, private val qTree: QTree): IQueryExecutor {
  private val mTable = tables.get(qTree.entity)
  private val qTable = table(mTable.sqlName()).asTable(MAIN)

  private val joinFields = mutableListOf<Field<Any>>()
  private val joinTables = mutableListOf<Pair<Table<*>, Condition>>()

  private var hasInClause = false
  private var conditions: Condition? = null

  private val inverted = linkedMapOf<String, SQLQueryExecutor>()

  // compile QTree
  init {
    qTree.run { mTable.compile(select, filter) }
  }

  override fun exec(params: Map<String, Any>): IResult {
    val mParams = params.toMutableMap()
    val query = db.selectQuery().apply {
      addSelect(joinFields)
      addFrom(qTable)

      joinTables.forEach { addJoin(it.first, it.second) }
      if (conditions != null) {
        // needs to rebuild conditions with included values when "IN" clause is present!
        if (hasInClause) addConditions(mTable.expression(qTree.filter!!, params = mParams)) else addConditions(conditions)
      }

      when {
        qTree.page != null -> { addLimit(qTree.limit); addOffset((qTree.page - 1) * qTree.limit!!) }
        qTree.limit != null -> addLimit(qTree.limit)
        else -> Unit
      }
    }

    println(query.sql)
    mParams.forEach { query.bind(it.key, it.value) }

    val data = TData()
    val directResult = query.fetch()
    directResult.forEach { data.process(it) }

    return SQLResult(data)
  }

  private fun STable.compile(selection: QSelect, filter: QExpression?) {
    joinFields(selection)
    joinTables(selection)
    if (filter != null) conditions = expression(filter)
  }

  @Suppress("UNCHECKED_CAST")
  private fun STable.joinFields(selection: QSelect, prefix: String = MAIN, alias: String = "") {
    val idField = if (prefix == MAIN) idFn(prefix) else idFn(prefix).`as`("$alias.$ID")
    joinFields.add(idField as Field<Any>)

    val fields = if (prefix == MAIN) dbFields(selection, prefix) else dbFields(selection, prefix).map { it.`as`("$alias.${it.name}") }
    joinFields.addAll(fields)

    val refs = dbDirectRefs(selection)
    for (qRel in refs.keys) {
      val rTable = tables.get(qRel.ref)
      rTable.joinFields(qRel.select, qRel.name, "$alias.${qRel.name}")
    }
  }

  private fun STable.joinTables(selection: QSelect, prefix: String = MAIN) {
    val refs = dbDirectRefs(selection)
    for (dRef in refs.values) {
      val rTable = table(dRef.rel.ref.sqlName()).asTable(dRef.rel.name)
      val rField = dRef.fn(this, prefix)
      val rId = idFn(dRef.rel.name)
      joinTables.add(rTable to rField.eq(rId))
    }

    for (qRel in refs.keys) {
      val nextTable = tables.get(qRel.ref)
      nextTable.joinTables(qRel.select, qRel.name)
    }
  }

  private fun STable.expression(expr: QExpression, prefix: String = MAIN, params: MutableMap<String, Any> = mutableMapOf()): Condition = if (expr.predicate != null) {
    predicate(expr.predicate, params)
  } else {
    val left = expression(expr.left!!, prefix)
    val right = expression(expr.right!!, prefix)

    when(expr.oper!!) {
      OperType.OR -> left.or(right)
      OperType.AND -> left.and(right)
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun predicate(pred: QPredicate, params: MutableMap<String, Any>, prefix: String = MAIN): Condition {
    // TODO: build a sub-query path!
    val subQuery = pred.path.first().name

    //mapper.params[subQuery] = predicate.param

    val path = field(name(prefix, subQuery))
    val value = if (pred.param.type == ParamType.PARAM) param(pred.param.value as String) else pred.param.value

    return when(pred.comp) {
      CompType.EQUAL -> path.eq(value)
      CompType.DIFFERENT -> path.ne(value)
      CompType.MORE -> path.greaterThan(value)
      CompType.LESS -> path.lessThan(value)
      CompType.MORE_EQ -> path.greaterOrEqual(value)
      CompType.LESS_EQ -> path.lessOrEqual(value)
      CompType.IN -> {
        hasInClause = true
        if (pred.param.type == ParamType.PARAM) {
          val inValues = params.remove(pred.param.value)
          path.`in`(inValues)
        } else {
          path.`in`(value)
        }
      }
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