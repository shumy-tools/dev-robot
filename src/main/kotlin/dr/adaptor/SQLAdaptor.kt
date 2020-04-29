package dr.adaptor

import com.zaxxer.hikari.HikariDataSource
import dr.io.Delete
import dr.io.Insert
import dr.io.Instructions
import dr.io.Update
import dr.query.*
import dr.schema.Schema
import dr.schema.tabular.*
import dr.schema.tabular.Table
import dr.spi.IAdaptor
import dr.spi.IQueryExecutor
import org.jooq.*
import org.jooq.conf.RenderQuotedNames
import org.jooq.conf.Settings
import org.jooq.impl.DSL.*
import org.jooq.impl.SQLDataType
import java.sql.SQLException
import java.sql.Statement

class SQLAdaptor(val schema: Schema, private val url: String): IAdaptor {
  private val tables: Tables = TParser(schema).transform()

  private val ds = HikariDataSource().also {
    it.jdbcUrl = url
    it.addDataSourceProperty("cachePrepStmts", true)
    it.addDataSourceProperty("prepStmtCacheSize", 5*tables.size)
    it.addDataSourceProperty("prepStmtCacheSqlLimit", 2048)
  }

  private val settings = Settings()
    .withRenderQuotedNames(RenderQuotedNames.ALWAYS)
    .withExecuteLogging(true)

  private val db = using(ds, SQLDialect.H2, settings)

  init {
    if (db.selectOne().fetch().isEmpty())
      throw SQLException("Database connection failed! - ($url)")
  }

  fun createSchema() {
    val allConstraints = linkedMapOf<Table, List<Constraint>>()
    tables.allTables().forEach { tlb ->
      val constraints = mutableListOf<Constraint>()
      allConstraints[tlb] = constraints

      var dbTable = db.createTable(table(tlb.sqlName()))
      for (prop in tlb.props) {
        if (prop.isUnique) constraints.add(unique(prop.name))

        dbTable = when(prop) {
          is TID -> dbTable.column(prop.name, SQLDataType.BIGINT.nullable(false).identity(true))
          is TType -> dbTable.column(prop.name, SQLDataType.VARCHAR.nullable(false))
          is TEmbedded -> dbTable.column(prop.name, SQLDataType.JSON.nullable(prop.rel.isOptional))
          is TField -> dbTable.column(prop.name, prop.field.type.toSqlType().nullable(prop.field.isOptional))
        }
      }

      for (ref in tlb.refs) {
        val rName = ref.fn(tlb)
        dbTable = dbTable.column(rName, SQLDataType.BIGINT) // TODO: is nullable?

        if (ref.isUnique) constraints.add(unique(rName))
        val fk = foreignKey(rName).references(table(ref.refTable.sqlName()))
        constraints.add(fk)
      }

      val dbFinal = dbTable.constraint(primaryKey(ID))
      println(dbFinal.sql)
      dbFinal.execute()
    }

    allConstraints.forEach { (tlb, cList) ->
      val dbAlterTable = db.alterTable(table(tlb.sqlName())).add(cList)
      println(dbAlterTable.sql)
      dbAlterTable.execute()
    }
  }

  override fun tables(): Tables = tables

  override fun commit(instructions: Instructions) {
    db.transaction { conf ->
      println("TX-START")
      instructions.exec { inst ->
        val tableName = inst.table.sqlName()
        when (inst) {
          is Insert -> {
            val fields = inst.data.keys.map { it.fn() }
            val refs = inst.resolvedRefs.keys.map { it.fn(inst.table) }
            val insert = using(conf)
              .insertInto(table(tableName), fields.plus(refs))
              .values(inst.data.values.plus(inst.resolvedRefs.values))
              //.returning(fn(SQL_ID))

            print("  ${insert.sql}")

            // FIX: jOOQ is not returning the generated key!!!
            //val id = insert.fetchOne() as Long? ?: throw Exception("Insert failed! No return $ID!")

            val stmt = conf.connectionProvider().acquire().prepareStatement(insert.sql, Statement.RETURN_GENERATED_KEYS)

            var seq = 1
            for (value in inst.data.values.plus(inst.resolvedRefs.values)) {
              stmt.setObject(seq, value)
              seq++
            }

            val affectedRows = stmt.executeUpdate()
            if (affectedRows == 0)
              throw Exception("Instruction failed, no rows affected!")

            val genKeys = stmt.generatedKeys
            val id = if (genKeys.next()) genKeys.getLong(1) else 0L
            println(" - $ID=$id")

            id
          }

          is Update -> {
            var dbUpdate = using(conf).update(table(tableName)) as UpdateSetMoreStep<*>
            inst.data.forEach { dbUpdate = dbUpdate.set(it.key.fn(), it.value) }
            inst.resolvedRefs.forEach { dbUpdate = dbUpdate.set(it.key.fn(inst.table), it.value) }
            val update = dbUpdate.where(idFn().eq(inst.id))

            println("  ${update.sql}")
            val affectedRows = update.execute()
            if (affectedRows == 0)
              throw Exception("Instruction failed, no rows affected!")

            0L
          }

          is Delete -> {
            // TODO: disable stuff?
            0L
          }
        }
      }
      println("TX-COMMIT")
    }
  }

  override fun compile(query: QTree): IQueryExecutor {
    val mTable = tables.get(query.entity)
    val mStruct = query.run { mTable.select(select, filter, limit, page) }
    return MultiQueryExecutor(mStruct)
  }

  private fun Table.select(selection: QSelect, filter: QExpression?, limit: Int?, page: Int?): QStruct {
    val allFields = mutableListOf<Field<Any>>()
    joinFields(MAIN, "", selection, allFields)

    val mTable = table(sqlName()).asTable(MAIN)
    val dbSelect = db.select(allFields).from(mTable)

    val dbJoin = joinTables(MAIN, dbSelect, selection, filter, limit, page)

    val dbFilter = mutableListOf<Condition>()
    if (filter != null) dbFilter.add(expression(filter))

    val dbFiltered = dbJoin.where(dbFilter)
    val dbFinal = when {
      page != null -> dbFiltered.limit(limit).offset(page * limit!!)
      limit != null -> dbFiltered.limit(limit)
      else -> dbFiltered
    }

    return QStruct(dbFinal)
  }

  @Suppress("UNCHECKED_CAST")
  private fun Table.joinFields(prefix: String, alias: String, selection: QSelect, joinFields: MutableList<Field<Any>>) {
    val idField = if (prefix == MAIN) idFn(prefix) else idFn(prefix).`as`("$alias.$ID")
    joinFields.add(idField as Field<Any>)

    val fields = if (prefix == MAIN) dbFields(selection, prefix) else dbFields(selection, prefix).map { it.`as`("$alias.${it.name}") }
    joinFields.addAll(fields)

    val refs = dbDirectRefs(selection)
    for (qRel in refs.keys) {
      val rTable = tables.get(qRel.ref)
      rTable.joinFields(qRel.name, "$alias.${qRel.name}", qRel.select, joinFields)
    }
  }

  private fun Table.joinTables(prefix: String, dbSelect: SelectJoinStep<*>, selection: QSelect, filter: QExpression?, limit: Int?, page: Int?): SelectJoinStep<*> {
    var nextJoin = dbSelect

    val refs = dbDirectRefs(selection)
    for (dRef in refs.values) {
      val rTable = table(dRef.rel.ref.sqlName()).asTable(dRef.rel.name)
      val rField = dRef.fn(this, prefix)
      val rId = idFn(dRef.rel.name)
      nextJoin = dbSelect.join(rTable).on(rField.eq(rId))
    }

    for (qRel in refs.keys) {
      val nextTable = tables.get(qRel.ref)
      nextJoin = nextTable.joinTables(qRel.name, nextJoin, qRel.select, qRel.filter, qRel.limit, qRel.page)
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

    val path = field(name(prefix, subQuery))
    val value = if (predicate.param.type == ParamType.PARAM) param(predicate.param.value as String) else predicate.param.value

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
/*private fun ResultSet.getField(seq: Int, type: FieldType): Any? = when(type) {
  TEXT -> getString(seq)
  INT -> getInt(seq)
  LONG -> getLong(seq)
  FLOAT -> getFloat(seq)
  DOUBLE -> getDouble(seq)
  BOOL -> getBoolean(seq)
  TIME -> getTime(seq).toLocalTime()
  DATE -> getDate(seq).toLocalDate()
  DATETIME -> getTimestamp(seq).toLocalDateTime()
}

private fun Tables.createSubQuery(db: DSLContext, topQuery: SubQuery, qRel: QRelation): SubQuery {
  //val sRelation = topTable.sEntity.rels[qRel.name]!!
  val rTable = get(qRel.entity)

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

  val includeIdFilter = topQuery.hasRefTo(rTable.sEntity)
  val sQuery = qRel.run { db.select(rTable, select, filter, limit, page, includeIdFilter) }
  return SubQuery(rTable, sQuery, includeIdFilter)
}

private class SubQueryResult {
  class Row {
    val selected = linkedMapOf<String, Any?>()
    val directRefs = linkedMapOf<String, Pair<SEntity, Long?>>()

    override fun toString() = selected.plus(directRefs.map { it.key to "(${it.value.first.name}:${it.value.second})" }).toString()
  }

  val ids = mutableSetOf<Long>()
  val rows = linkedMapOf<Long, Row>()
}

private class SubQuery(val tlb: Table, val query: Select<*>, val includeIdFilter: Boolean = false) {
  val sEntity: SEntity
    get() = tlb.sEntity

  lateinit var idFilter: List<Long>

  fun hasRefTo(sEntity: SEntity): Boolean {
    return tlb.refs.filterIsInstance<TDirectRef>().any { it.rel.ref == sEntity }
  }

  fun exec(params: Map<String, Any>): Result<*> {
    println("SUB-QUERY: ${query.sql} ${params.values}")
    params.forEach { query.bind(it.key, it.value) }

    //if (includeIdFilter)
    //  query.bind("id-filter", idFilter)

    val result = query.fetch()
    println(result)

    return result
  }
}

private class MultiQueryExecutor(val ds: HikariDataSource, val main: SubQuery, val qTree: Map<String, SubQuery>): IQueryExecutor {
  override fun exec(params: Map<String, Any>): IResult {
    val mRes = main.exec(params)

    for ((relName, sQuery) in qTree) {
      if (sQuery.includeIdFilter)
        sQuery.idFilter = mRes.map { it[SQL_ID]!! as Long }

      val sRes = sQuery.exec(params)
    }

    return SQLResult(emptyList())
  }
}

private class SQLResult(val raw: Rows): IResult {
  override fun <T : Any> get(name: String): T {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
  }

  override fun raw() = raw
}
*/
