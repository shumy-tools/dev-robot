package dr.adaptor

import dr.io.JsonParser
import dr.query.*
import dr.schema.tabular.ID
import dr.schema.tabular.STable
import dr.schema.tabular.TRAITS
import dr.schema.tabular.Tables
import dr.spi.IQueryExecutor
import dr.spi.IResult
import dr.spi.QRow
import org.jooq.*
import org.jooq.impl.DSL.*
import java.util.concurrent.atomic.AtomicBoolean

class SQLQueryExecutor(private val db: DSLContext, private val tables: Tables, private val qTree: QTree): IQueryExecutor {
  private val qTable = table(qTree.table.sqlName()).asTable(MAIN)

  private val joinFields = mutableListOf<Field<Any>>()
  private val joinTables = mutableListOf<Pair<Table<*>, Condition>>()

  private var hasInClause = AtomicBoolean(false)
  private var conditions: Condition? = null

  private val inverted = linkedMapOf<String, Pair<Field<Long>, SQLQueryExecutor>>()

  // compile query tree
  init { qTree.table.compile(qTree.select, qTree.filter) }

  override fun exec(params: Map<String, Any>): IResult {
    return subExec(params)
  }

  @Suppress("UNCHECKED_CAST")
  fun subExec(params: Map<String, Any>, fk: Field<Long>? = null, topIds: Select<*>? = null): IResult {
    val mParams = params.toMutableMap()
    val ids = buildQueryIds(params.toMutableMap())
    val query = buildQuery(mParams)

    if (fk != null) {
      query.addSelect(fk)
      query.addConditions((fk as Field<Any>).`in`(topIds))

      // TODO: add refTable conditions?
    }

    // process results
    val result = SQLResult(tables)

    // add main result
    println(query.sql)
    mParams.forEach { query.bind(it.key, it.value) }
    query.fetch().forEach { result.process(it, fk) }

    // add one-to-many and many-to-many results
    inverted.forEach {
      val subResult = it.value.second.subExec(params, it.value.first, ids) as SQLResult
      result.rows.keys.forEach { pk ->
        val fkKeys = subResult.fkKeys[pk] // join results via "pk <-- fk"
        fkKeys?.forEach { subID ->
          val line = subResult.rows[subID]!!
          result.addTo(pk, it.key, line)
        }
      }
    }

    // TODO: add TSuperRef results

    return result
  }

  private fun STable.compile(selection: QSelect, filter: QExpression?) {
    joinFields(selection)
    joinTables(selection)

    if (filter != null) conditions = expression(filter)

    // compile one-to-many relations
    val oneToMany = qTree.table.dbOneToMany(qTree.select)
    for ((qRel, iRef) in oneToMany) {
      // add sub-query @A.id <-- B.inv_<A>_<rel>
      val refTable = tables.get(iRef.rel.ref)
      val subTree = QTree(refTable, qRel)

      val subQuery = SQLQueryExecutor(db, tables, subTree)
      inverted[qRel.name] = Pair(iRef.fn(refTable, MAIN), subQuery)
      // TODO: process one-to-many with traits?
    }

    // compile many-to-many relations
    val manyToMany = qTree.table.dbManyToMany(qTree.select)
    for ((qRel, iRef) in manyToMany) {
      println(qRel)

      // simulate one-to-many via @A.id <-- @AX.inv (one-to-one AX.@ref --> B.@id)
      val refFields = qRel.select.fields.filter { iRef.second.fields[it.name] != null }
      val refSelect = QSelect(qRel.select.hasAll, refFields, emptyList())
      val refRel = qRel.select.relations.plus(QRelation(qRel.name, qRel.ref, qRel.filter/* TODO: only refTable filter */, null, null, refSelect))

      // TODO: include aux-table traits (select and filter)
      val auxTable = tables.get(iRef.first.sEntity, iRef.first.sEntity.rels[qRel.name])
      val auxFields = qRel.select.fields.filter { auxTable.props[it.name] != null }
      val auxSelect = QSelect(qRel.select.hasAll, auxFields, refRel)
      val auxTree = QTree(auxTable, null/* TODO: filter */, qRel.limit, qRel.page, auxSelect)


      val subQuery = SQLQueryExecutor(db, tables, auxTree)
      inverted[qRel.name] = Pair(invFn(MAIN), subQuery)
    }
  }

  private fun buildQueryIds(mParams: MutableMap<String, Any>) = db.selectQuery().apply {
    addSelect(idFn())
    buildQueryParts(mParams)
  }

  private fun buildQuery(mParams: MutableMap<String, Any>) = db.selectQuery().apply {
    addSelect(joinFields)
    buildQueryParts(mParams)
  }

  private fun SelectQuery<Record>.buildQueryParts(mParams: MutableMap<String, Any>) {
    addFrom(qTable)

    joinTables.forEach { addJoin(it.first, it.second) }
    if (conditions != null) {
      // needs to rebuild conditions with included values when "IN" clause is present!
      if (hasInClause.get()) addConditions(qTree.table.expression(qTree.filter!!, params = mParams)) else addConditions(conditions)
    }

    when {
      qTree.page != null -> { addLimit(qTree.limit); addOffset((qTree.page - 1) * qTree.limit!!) }
      qTree.limit != null -> addLimit(qTree.limit)
      else -> Unit
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun STable.joinFields(selection: QSelect, prefix: String = MAIN, alias: String = "") {
    val idField = if (prefix == MAIN) idFn(prefix) else idFn(prefix).`as`("$alias.$ID")
    joinFields.add(idField as Field<Any>)

    val fields = if (prefix == MAIN) dbFields(selection, prefix) else dbFields(selection, prefix).map { it.`as`("$alias.${it.name}") }
    joinFields.addAll(fields)

    val refs = dbOneToOne(selection)
    for (qRel in refs.keys) {
      qRel.ref.joinFields(qRel.select, qRel.name, "$alias.${qRel.name}")
    }
  }

  private fun STable.joinTables(selection: QSelect, prefix: String = MAIN) {
    // one-to-one A.@ref_<rel> --> B.@id
    val refs = dbOneToOne(selection)
    for (dRef in refs.values) {
      val rTable = table(dRef.rel.ref.sqlName()).asTable(dRef.rel.name)
      val rField = dRef.fn(this, prefix)
      val rId = idFn(dRef.rel.name)
      joinTables.add(rTable to rField.eq(rId))
    }

    for (qRel in refs.keys) {
      qRel.ref.joinTables(qRel.select, qRel.name)
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
        hasInClause.set(true)
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
class SQLResult(private val tables: Tables): IResult {
  internal val rows = linkedMapOf<Long, LinkedHashMap<String, Any?>>()
  internal val fkKeys = linkedMapOf<Long, MutableList<Long>>()

  override fun row(id: Long) = rows[id]

  override fun rows() = rows.values.toList()

  override fun <T : Any> get(name: String): T {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
  }

  @Suppress("UNCHECKED_CAST")
  fun addTo(pk: Long, name: String, row: QRow) {
    val main = rows.getValue(pk)
    val list = main.getOrPut(name) { mutableListOf<QRow>() } as MutableList<QRow>
    list.add(row)
  }

  fun process(record: Record, fk: Field<Long>? = null) {
    val fieldID = record.fields().find { it.name == ID }!!
    val rowID = (fieldID.getValue(record) as Long?)!!
    val row = linkedMapOf<String, Any?>()
    rows[rowID] = row

    record.fields().forEach {
      val value = it.getValue(record)

      // process foreignKeys
      if (fk != null && it.name == fk.name) {
        val fkRows = fkKeys.getOrPut(value as Long) { mutableListOf() }
        fkRows.add(rowID)
      } else {
        if (it.name.startsWith('.')) row.processJoin(it.name, value) else row.setField(it.name, value)
      }
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun LinkedHashMap<String, Any?>.processJoin(name: String, value: Any?) {
    val splits = name.split('.').drop(1)

    var place = this
    for (position in splits.dropLast(1)) {
      place = place.getOrPut(position) { linkedMapOf<String, Any?>() } as LinkedHashMap<String, Any?>
    }

    place.setField(splits.last(), value)
  }

  private fun LinkedHashMap<String, Any?>.setField(name: String, value: Any?) {
    if (value == null) {
      this[name] = null
      return
    }

    val cValue = if (name.startsWith(TRAITS)) {
      val trait = name.substring(1)
      val sTrait = tables.schema.traits.getValue(trait)
      JsonParser.read((value as String), sTrait.clazz)
    } else value

    this[name] = cValue
  }
}