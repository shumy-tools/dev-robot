package dr.adaptor

import com.zaxxer.hikari.HikariDataSource
import dr.io.Delete
import dr.io.Insert
import dr.io.Instructions
import dr.io.Update
import dr.query.QRelation
import dr.query.QTree
import dr.schema.FieldType
import dr.schema.FieldType.*
import dr.schema.SEntity
import dr.schema.Schema
import dr.schema.tabular.*
import dr.schema.tabular.Table
import dr.spi.IAdaptor
import dr.spi.IQueryExecutor
import dr.spi.IResult
import dr.spi.Rows
import org.jooq.*
import org.jooq.conf.RenderQuotedNames
import org.jooq.conf.Settings
import org.jooq.impl.DSL.table
import org.jooq.impl.DSL.using
import java.sql.ResultSet
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
    ds.connection.use { conn ->
      tables.allTables().forEach { inst ->
        val sql = inst.createSql()
        println(sql)
        conn.createStatement().use { it.execute(sql) }
      }

      tables.allTables().forEach { inst ->
        inst.refs.forEach { ref ->
          val sql = inst.foreignKeySql(ref)
          println(sql)
          conn.createStatement().use { it.execute(sql) }
        }
      }
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
            val fields = inst.data.keys.map { fn(propSqlName(it)) }
            val refs = inst.resolvedRefs.keys.map { fn(inst.table.refSqlName(it)) }
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
            inst.data.forEach { dbUpdate = dbUpdate.set(fn(propSqlName(it.key)), it.value) }
            inst.resolvedRefs.forEach { dbUpdate = dbUpdate.set(fn(inst.table.refSqlName(it.key)), it.value) }
            val update = dbUpdate.where(fn(SQL_ID).eq(inst.id))

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
    // process main query
    val mTable = tables.get(query.entity)
    val mQuery = query.run { db.select(mTable, select, filter, limit, page) }
    val mSubQuery = SubQuery(mTable, mQuery)

    // process sub-queries
    val queries = linkedMapOf<String, SubQuery>()
    for (qRel in query.select.relations) {
      queries[qRel.name] = tables.createSubQuery(db, mSubQuery, qRel)
    }

    return MultiQueryExecutor(ds, mSubQuery, queries)
  }
}

/* ------------------------- helpers -------------------------*/
private fun ResultSet.getField(seq: Int, type: FieldType): Any? = when(type) {
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


